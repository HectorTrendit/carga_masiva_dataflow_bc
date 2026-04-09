package com.bancopel.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Paso1RecordMapper {
  private static final DateTimeFormatter FLEXIBLE_TS = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart()
      .appendPattern(".SSS")
      .optionalEnd()
      .toFormatter();

  private Paso1RecordMapper() {}

  public static Paso1Record fromJson(JsonNode obj, String source) {
    String approxLogTime = parseTs(textOf(obj, "approxLogTime"));
    String ingestTime = parseTs(textOf(obj, "ingest_time"));
    String collectorTs = parseTs(textOf(obj, "collector_timestamp"));
    String rowCreateTime = parseTs(textOf(obj, "row_create_time"));
    String ingestDate = null;
    if (ingestTime != null && ingestTime.length() >= 10) {
      ingestDate = ingestTime.substring(0, 10);
    }

    Long rawLogSize = null;
    String rawLogSizeText = textOf(obj, "raw_log_size");
    if (rawLogSizeText != null) {
      try {
        rawLogSize = Long.parseLong(rawLogSizeText);
      } catch (NumberFormatException ignored) {
        rawLogSize = null;
      }
    }

    return new Paso1Record(
        textOf(obj, "id"),
        approxLogTime,
        ingestTime,
        ingestDate,
        collectorTs,
        rowCreateTime,
        textOf(obj, "tier"),
        rawLogSize,
        booleanValue(obj, "parsed"),
        arrayText(obj, "rawLogIds"),
        arrayText(obj, "rawLogs"),
        obj.path("metadataFieldsJSON").toString(),
        obj.path("event_debug_info").toString(),
        obj.toString(),
        source
    );
  }

  public static DeadletterRecord deadletter(String source, long index, Exception e) {
    return new DeadletterRecord(
        source,
        index,
        e.toString(),
        "parse",
        Instant.now().toString(),
        null
    );
  }

  public static DeadletterRecord deadletterWithPayload(
      String source, long index, Exception e, JsonNode payload) {
    return new DeadletterRecord(
        source,
        index,
        e.toString(),
        "map",
        Instant.now().toString(),
        payload == null ? null : payload.toString()
    );
  }

  private static String parseTs(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    String text = value.trim();
    if (text.endsWith(" UTC")) {
      text = text.substring(0, text.length() - 4);
    }
    try {
      return Instant.parse(text).toString();
    } catch (DateTimeParseException ignored) {
    }
    try {
      return OffsetDateTime.parse(text).toInstant().toString();
    } catch (DateTimeParseException ignored) {
    }
    try {
      LocalDateTime ldt = LocalDateTime.parse(text, FLEXIBLE_TS);
      return ldt.atZone(ZoneOffset.UTC).toInstant().toString();
    } catch (DateTimeParseException ignored) {
    }
    return null;
  }

  private static String textOf(JsonNode obj, String field) {
    JsonNode node = obj.get(field);
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  private static Boolean booleanValue(JsonNode obj, String field) {
    JsonNode node = obj.get(field);
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asBoolean();
  }

  private static List<String> arrayText(JsonNode obj, String field) {
    JsonNode node = obj.get(field);
    if (node == null || !node.isArray()) {
      return Collections.emptyList();
    }
    List<String> out = new ArrayList<>(node.size());
    for (int i = 0; i < node.size(); i++) {
      out.add(node.get(i).asText());
    }
    return out;
  }
}
