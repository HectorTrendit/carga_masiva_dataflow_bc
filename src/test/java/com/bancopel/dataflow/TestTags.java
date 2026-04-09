package com.bancopel.dataflow;

import org.apache.beam.sdk.values.TupleTag;

public final class TestTags {
  private TestTags() {}

  public static final TupleTag<Paso1Record> MAIN = new TupleTag<Paso1Record>() {};
  public static final TupleTag<DeadletterRecord> DEAD = new TupleTag<DeadletterRecord>() {};
}
