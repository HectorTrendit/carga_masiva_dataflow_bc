package com.bancopel.dataflow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class CheckSchemaFn extends DoFn<Paso1Record, Paso1Record> {
  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());

  private final TupleTag<DeadletterRecord> deadTag;
  private final List<String> allowedFields;
  private transient Set<String> allowedSet;

  public CheckSchemaFn(TupleTag<DeadletterRecord> deadTag, List<String> allowedFields) {
    this.deadTag = deadTag;
    this.allowedFields = allowedFields;
  }

  @Setup
  public void setup() {
    this.allowedSet = new HashSet<>(allowedFields);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Paso1Record record = c.element();
    String payload = record.getPayload();
    if (payload == null || payload.isEmpty()) {
      c.output(record);
      return;
    }

    try {
      JsonNode node = MAPPER.readTree(payload);
      if (!node.isObject()) {
        c.output(deadTag, new DeadletterRecord(
            record.getSourceFile(),
            null,
            "payload_not_object",
            "schema",
            Instant.now().toString(),
            payload
        ));
        return;
      }

      List<String> unknown = new ArrayList<>();
      node.fieldNames().forEachRemaining(name -> {
        if (!allowedSet.contains(name)) {
          if (unknown.size() < 50) {
            unknown.add(name);
          }
        }
      });

      if (!unknown.isEmpty()) {
        String msg = "unknown_fields:" + String.join(",", unknown);
        c.output(deadTag, new DeadletterRecord(
            record.getSourceFile(),
            null,
            msg,
            "schema",
            Instant.now().toString(),
            payload
        ));
        return;
      }
    } catch (Exception e) {
      c.output(deadTag, new DeadletterRecord(
          record.getSourceFile(),
          null,
          "schema_check_failed:" + e.getClass().getSimpleName(),
          "schema",
          Instant.now().toString(),
          payload
      ));
      return;
    }

    c.output(record);
  }
}
