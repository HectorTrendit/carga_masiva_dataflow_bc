package com.bancopel.dataflow;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
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

  public TupleTag<Paso1Record> getMainTag() {
    return mainTag;
  }

  public TupleTag<DeadletterRecord> getDeadTag() {
    return deadTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<FileIO.ReadableFile> input) {
    // Para escalar a 400 TB, usamos TextIO.readFiles() que permite que Dataflow
    // divida archivos grandes (como los de 4GB) en múltiples bloques paralelos.
    PCollection<String> lines = input.apply("ReadLines", TextIO.readFiles());

    PCollectionTuple parsed = lines
        .apply("ParseLine", ParDo.of(new ParseLineFn(parseDeadTag, "GCS_SOURCE"))
            .withOutputTags(mainTag, TupleTagList.of(parseDeadTag)));

    return PCollectionTuple.of(mainTag, parsed.get(mainTag))
        .and(deadTag, parsed.get(parseDeadTag));
  }
}
