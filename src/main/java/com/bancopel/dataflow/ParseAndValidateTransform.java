package com.bancopel.dataflow;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class ParseAndValidateTransform
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollectionTuple> {

  private final TupleTag<Paso1Record> mainTag = new TupleTag<Paso1Record>() {};
  private final TupleTag<DeadletterRecord> deadTag = new TupleTag<DeadletterRecord>() {};
  private final TupleTag<DeadletterRecord> parseDeadTag = new TupleTag<DeadletterRecord>() {};
  private final TupleTag<DeadletterRecord> validateDeadTag = new TupleTag<DeadletterRecord>() {};

  public TupleTag<Paso1Record> getMainTag() {
    return mainTag;
  }

  public TupleTag<DeadletterRecord> getDeadTag() {
    return deadTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<FileIO.ReadableFile> input) {
    // ParseFileFn conserva el archivo origen y el índice del registro.
    PCollectionTuple parsed = input
        .apply("ParseFile", ParDo.of(new ParseFileFn(parseDeadTag))
            .withOutputTags(mainTag, TupleTagList.of(parseDeadTag)));

    PCollectionTuple validated = parsed.get(mainTag)
        .apply("ValidateRecord", ParDo.of(new ValidateRecordFn(validateDeadTag))
            .withOutputTags(mainTag, TupleTagList.of(validateDeadTag)));

    PCollection<DeadletterRecord> deadletters = PCollectionList
        .of(parsed.get(parseDeadTag))
        .and(validated.get(validateDeadTag))
        .apply("MergeDeadletters", Flatten.pCollections());

    return PCollectionTuple.of(mainTag, validated.get(mainTag))
        .and(deadTag, deadletters);
  }
}
