package com.bancopel.dataflow;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.TupleTag;

public class ParseAndValidateFn extends DoFn<FileIO.ReadableFile, Paso1Record> {
  private static final int READ_BUFFER_SIZE = 64 * 1024;
  private static final long SPLIT_SIZE_BYTES = 64L * 1024L * 1024L;

  private final TupleTag<Paso1Record> mainTag;
  private final TupleTag<DeadletterRecord> deadTag;

  public ParseAndValidateFn(TupleTag<Paso1Record> mainTag, TupleTag<DeadletterRecord> deadTag) {
    this.mainTag = mainTag;
    this.deadTag = deadTag;
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element FileIO.ReadableFile file) {
    long size = file.getMetadata().sizeBytes();
    return new OffsetRange(0L, size);
  }

  @SplitRestriction
  public void splitRestriction(
      @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> outputReceiver) {
    List<OffsetRange> splits = restriction.split(SPLIT_SIZE_BYTES, 1L);
    for (OffsetRange split : splits) {
      outputReceiver.output(split);
    }
  }

  @NewTracker
  public OffsetRangeTracker newTracker(@Restriction OffsetRange restriction) {
    return restriction.newTracker();
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> getRestrictionCoder() {
    return OffsetRange.Coder.of();
  }

  @GetSize
  public double getSize(@Restriction OffsetRange restriction) {
    return (double) (restriction.getTo() - restriction.getFrom());
  }

  @ProcessElement
  public void processElement(
      @Element FileIO.ReadableFile file,
      RestrictionTracker<OffsetRange, Long> tracker,
      MultiOutputReceiver receiver)
      throws IOException {
    String source = file.getMetadata().resourceId().toString();
    OffsetRange restriction = tracker.currentRestriction();
    long start = restriction.getFrom();
    long end = restriction.getTo();
    Position position = new Position(start);

    try (SeekableByteChannel channel = file.openSeekable()) {
      if (start > 0) {
        channel.position(start - 1);
        ByteBuffer previous = ByteBuffer.allocate(1);
        int read = channel.read(previous);
        boolean aligned = read <= 0;
        if (read > 0) {
          previous.flip();
          aligned = previous.get() == '\n';
        }
        channel.position(start);
        InputStream rawIn = Channels.newInputStream(channel);
        try (BufferedInputStream in = new BufferedInputStream(rawIn, READ_BUFFER_SIZE)) {
          if (!aligned) {
            discardToNextLine(in, position);
          }
          if (position.value >= end) {
            finishRestriction(tracker, end);
            return;
          }
          readRecords(source, tracker, receiver, in, position, end);
        }
      } else {
        InputStream rawIn = Channels.newInputStream(channel);
        try (BufferedInputStream in = new BufferedInputStream(rawIn, READ_BUFFER_SIZE)) {
          readRecords(source, tracker, receiver, in, position, end);
        }
      }
    }
  }

  private void readRecords(
      String source,
      RestrictionTracker<OffsetRange, Long> tracker,
      MultiOutputReceiver receiver,
      BufferedInputStream in,
      Position position,
      long end)
      throws IOException {
    while (true) {
      long lineStart = position.value;
      if (!tracker.tryClaim(lineStart)) {
        break;
      }

      String line = readLine(in, position);
      if (line == null) {
        finishRestriction(tracker, end);
        break;
      }
      if (line.trim().isEmpty()) {
        if (position.value >= end) {
          finishRestriction(tracker, end);
          break;
        }
        continue;
      }

      try {
        Paso1Record record = Paso1RecordMapper.fromJson(line, source);
        String validationError = ValidateRecordFn.validationError(record);
        if (validationError != null) {
          receiver.get(deadTag).output(ValidateRecordFn.deadletter(record, validationError));
          continue;
        }
        receiver.get(mainTag).output(record);
      } catch (Exception e) {
        receiver.get(deadTag).output(Paso1RecordMapper.deadletter(source, lineStart, e, line, "parse"));
      }

      if (position.value >= end) {
        finishRestriction(tracker, end);
        break;
      }
    }
  }

  private static void finishRestriction(RestrictionTracker<OffsetRange, Long> tracker, long end) {
    if (end <= 0) {
      return;
    }
    tracker.tryClaim(end);
  }

  private static void discardToNextLine(BufferedInputStream in, Position position) throws IOException {
    while (true) {
      int b = in.read();
      if (b == -1) {
        return;
      }
      position.value++;
      if (b == '\n') {
        return;
      }
    }
  }

  private static String readLine(BufferedInputStream in, Position position) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    boolean sawAny = false;
    while (true) {
      int b = in.read();
      if (b == -1) {
        return sawAny ? buffer.toString(StandardCharsets.UTF_8) : null;
      }
      sawAny = true;
      position.value++;
      if (b == '\n') {
        break;
      }
      if (b != '\r') {
        buffer.write(b);
      }
    }
    return buffer.toString(StandardCharsets.UTF_8);
  }

  private static final class Position {
    private long value;

    private Position(long value) {
      this.value = value;
    }
  }
}
