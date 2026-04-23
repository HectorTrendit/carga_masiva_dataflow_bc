package com.bancopel.dataflow;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class ParseAndValidateTransform
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollectionTuple> {

  private final TupleTag<Paso1Record> mainTag = new TupleTag<Paso1Record>() {};
  private final TupleTag<DeadletterRecord> deadTag = new TupleTag<DeadletterRecord>() {};

  public TupleTag<Paso1Record> getMainTag() {
    return mainTag;
  }

  public TupleTag<DeadletterRecord> getDeadTag() {
    return deadTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<FileIO.ReadableFile> input) {
    return input
        .apply("ParseAndValidate", ParDo.of(new ParseAndValidateFn(mainTag, deadTag))
            .withOutputTags(mainTag, TupleTagList.of(deadTag)));
  }
}
