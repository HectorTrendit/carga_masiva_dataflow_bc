package com.bancopel.dataflow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ParseFileFn extends DoFn<FileIO.ReadableFile, Paso1Record> {
  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());

  private final TupleTag<DeadletterRecord> deadTag;

  public ParseFileFn(TupleTag<DeadletterRecord> deadTag) {
    this.deadTag = deadTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FileIO.ReadableFile file = c.element();
    String source = file.getMetadata().resourceId().toString();
    long index = 0L;

    try (InputStream in = Channels.newInputStream(file.open())) {
      MappingIterator<JsonNode> it = MAPPER.readerFor(JsonNode.class).readValues(in);
      while (it.hasNextValue()) {
        try {
          JsonNode obj = it.nextValue();
          try {
            Paso1Record record = Paso1RecordMapper.fromJson(obj, source);
            c.output(record);
          } catch (Exception e) {
            c.output(deadTag, Paso1RecordMapper.deadletterWithPayload(source, index, e, obj));
          }
        } catch (Exception e) {
          c.output(deadTag, Paso1RecordMapper.deadletter(source, index, e));
        }
        index++;
      }
    } catch (Exception e) {
      c.output(deadTag, Paso1RecordMapper.deadletter(source, index, e));
    }
  }
}
