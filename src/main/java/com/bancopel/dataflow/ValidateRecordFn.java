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
    String reason = validationError(record);
    if (reason != null) {
      c.output(deadTag, buildDead(record, reason));
      return;
    }

    c.output(record);
  }

  public static String validationError(Paso1Record record) {
    if (record == null) {
      return "record_missing";
    }
    if (isBlank(record.getId())) {
      return "id_missing";
    }
    if (isBlank(record.getTier())) {
      return "tier_missing";
    }
    if (isBlank(record.getPayload())) {
      return "payload_missing";
    }
    if (isBlank(record.getIngestDate())) {
      return "ingest_date_missing";
    }
    try {
      LocalDate.parse(record.getIngestDate());
    } catch (DateTimeParseException e) {
      return "ingest_date_invalid";
    }
    Long rawLogSize = record.getRawLogSize();
    if (rawLogSize != null && rawLogSize < 0) {
      return "raw_log_size_negative";
    }
    return null;
  }

  public static DeadletterRecord deadletter(Paso1Record record, String reason) {
    return new DeadletterRecord(
        record.getSourceFile(),
        null,
        reason,
        STAGE,
        Instant.now().toString(),
        record.getPayload()
    );
  }

  private DeadletterRecord buildDead(Paso1Record record, String reason) {
    return deadletter(record, reason);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
