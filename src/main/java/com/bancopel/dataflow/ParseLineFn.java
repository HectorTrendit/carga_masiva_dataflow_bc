package com.bancopel.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ParseLineFn extends DoFn<String, Paso1Record> {

  private final TupleTag<DeadletterRecord> deadTag;
  private final String source;

  public ParseLineFn(TupleTag<DeadletterRecord> deadTag, String source) {
    this.deadTag = deadTag;
    this.source = source;
  }

  @ProcessElement
  public void processElement(@Element String line, ProcessContext c) {
    if (line == null || line.trim().isEmpty()) {
      return;
    }

    try {
      Paso1Record record = Paso1RecordMapper.fromJson(line, source);
      c.output(record);
    } catch (Exception e) {
      // In a line-based processing, we don't have the original index easily unless we pass it.
      // For now, using 0 or a placeholder.
      c.output(deadTag, Paso1RecordMapper.deadletter(source, 0L, e, line));
    }
  }
}
