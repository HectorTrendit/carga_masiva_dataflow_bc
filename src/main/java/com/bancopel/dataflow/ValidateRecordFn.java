package com.bancopel.dataflow;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ValidateRecordFn extends DoFn<Paso1Record, Paso1Record> {
  private final TupleTag<DeadletterRecord> deadTag;

  public ValidateRecordFn(TupleTag<DeadletterRecord> deadTag) {
    this.deadTag = deadTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Paso1Record record = c.element();

    String payload = record.getPayload();
    if (payload == null || payload.isEmpty()) {
      c.output(deadTag, buildDead(record, "payload_missing"));
      return;
    }

    String ingestDate = record.getIngestDate();
    if (ingestDate != null && !ingestDate.isEmpty()) {
      try {
        LocalDate.parse(ingestDate);
      } catch (DateTimeParseException e) {
        c.output(deadTag, buildDead(record, "ingest_date_invalid"));
        return;
      }
    }

    c.output(record);
  }

  private DeadletterRecord buildDead(Paso1Record record, String reason) {
    return new DeadletterRecord(
        record.getSourceFile(),
        null,
        reason,
        "validate",
        Instant.now().toString(),
        record.getPayload()
    );
  }
}
