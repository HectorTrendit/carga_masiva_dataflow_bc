package com.bancopel.dataflow;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ValidateRecordFn extends DoFn<Paso1Record, Paso1Record> {
  private static final String STAGE = "validate";
  private final TupleTag<DeadletterRecord> deadTag;

  public ValidateRecordFn(TupleTag<DeadletterRecord> deadTag) {
    this.deadTag = deadTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Paso1Record record = c.element();

    if (isBlank(record.getId())) {
      c.output(deadTag, buildDead(record, "id_missing"));
      return;
    }

    if (isBlank(record.getTier())) {
      c.output(deadTag, buildDead(record, "tier_missing"));
      return;
    }

    String payload = record.getPayload();
    if (isBlank(payload)) {
      c.output(deadTag, buildDead(record, "payload_missing"));
      return;
    }

    String ingestDate = record.getIngestDate();
    if (isBlank(ingestDate)) {
      c.output(deadTag, buildDead(record, "ingest_date_missing"));
      return;
    }

    try {
      LocalDate.parse(ingestDate);
    } catch (DateTimeParseException e) {
      c.output(deadTag, buildDead(record, "ingest_date_invalid"));
      return;
    }

    Long rawLogSize = record.getRawLogSize();
    if (rawLogSize != null && rawLogSize < 0) {
      c.output(deadTag, buildDead(record, "raw_log_size_negative"));
      return;
    }

    c.output(record);
  }

  private DeadletterRecord buildDead(Paso1Record record, String reason) {
    return new DeadletterRecord(
        record.getSourceFile(),
        null,
        reason,
        STAGE,
        Instant.now().toString(),
        record.getPayload()
    );
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
