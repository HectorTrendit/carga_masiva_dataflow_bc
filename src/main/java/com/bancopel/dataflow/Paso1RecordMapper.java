package com.bancopel.dataflow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Paso1RecordMapper {
  private static final ZoneId INGEST_DATE_ZONE = ZoneId.of("America/Mexico_City");
  private static final DateTimeFormatter FLEXIBLE_TS = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart()
      .appendPattern(".SSS")
      .optionalEnd()
      .toFormatter();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RawPayload {
    public Object id;
    public Object approxLogTime;
    @JsonProperty("ingest_time")
    public Object ingestTime;
    @JsonProperty("collector_timestamp")
    public Object collectorTimestamp;
    @JsonProperty("row_create_time")
    public Object rowCreateTime;
    public Object tier;
    @JsonProperty("raw_log_size")
    public Object rawLogSize;
    public Object parsed;
    public Object rawLogIds;
    public Object rawLogs;
    @JsonProperty("metadataFieldsJSON")
    public Object metadataFieldsJSON;
    @JsonProperty("event_debug_info")
    public Object eventDebugInfo;
    @JsonProperty("customFieldsJSON")
    public Object customFieldsJSON;
  }

  private Paso1RecordMapper() {}
  public static Paso1Record fromJson(String line, String source) throws Exception {
    RawPayload obj = MAPPER.readValue(line, RawPayload.class);
    return buildRecord(obj, source, line);
  }

  public static Paso1Record fromJson(JsonNode node, String source) throws Exception {
    String line = node.toString();
    RawPayload obj = MAPPER.treeToValue(node, RawPayload.class);
    return buildRecord(obj, source, line);
  }

  private static Paso1Record buildRecord(RawPayload obj, String source, String line) throws Exception {
    String approxLogTime = parseTs(textOf(obj.approxLogTime));
    String ingestTime = parseTs(textOf(obj.ingestTime));
    String collectorTs = parseTs(textOf(obj.collectorTimestamp));
    String rowCreateTime = parseTs(textOf(obj.rowCreateTime));
    String ingestDate = deriveIngestDate(ingestTime, collectorTs);

    Long rawLogSize = null;
    String rawLogSizeText = textOf(obj.rawLogSize);
    if (rawLogSizeText != null) {
      try {
        rawLogSize = Long.parseLong(rawLogSizeText);
      } catch (NumberFormatException ignored) {
        rawLogSize = null;
      }
    }

    return new Paso1Record(
        textOf(obj.id),
        approxLogTime,
        ingestTime,
        ingestDate,
        collectorTs,
        rowCreateTime,
        textOf(obj.tier),
        rawLogSize,
        booleanValue(obj.parsed),
        arrayText(obj.rawLogIds),
        arrayText(obj.rawLogs),
        jsonOf(obj.metadataFieldsJSON),
        jsonOf(obj.eventDebugInfo),
        jsonOf(obj.customFieldsJSON),
        line,
        source
    );
  }

  public static DeadletterRecord deadletter(String source, Long index, Exception e) {
    return deadletter(source, index, e, null, "parse");
  }

  public static DeadletterRecord deadletter(String source, Long index, Exception e, String rawText) {
    return deadletter(source, index, e, rawText, "parse");
  }

  public static DeadletterRecord deadletter(String source, Long index, Exception e, String rawText, String stage) {
    return new DeadletterRecord(
        source,
        index,
        e.toString(),
        stage,
        Instant.now().toString(),
        rawText
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

  private static String deriveIngestDate(String ingestTime, String collectorTs) {
    if (ingestTime != null && ingestTime.length() >= 10) {
      return ingestTime.substring(0, 10);
    }
    if (collectorTs != null) {
      try {
        return Instant.parse(collectorTs)
            .atZone(INGEST_DATE_ZONE)
            .toLocalDate()
            .toString();
      } catch (DateTimeParseException ignored) {
      }
    }
    return null;
  }

  private static String textOf(Object val) {
    if (val == null) return null;
    return String.valueOf(val);
  }

  private static Boolean booleanValue(Object val) {
    if (val == null) return null;
    if (val instanceof Boolean) return (Boolean) val;
    String text = String.valueOf(val).toLowerCase();
    return "true".equals(text) || "1".equals(text);
  }

  private static List<String> arrayText(Object val) {
    if (val == null) return Collections.emptyList();
    if (val instanceof List) {
      List<?> list = (List<?>) val;
      List<String> out = new ArrayList<>(list.size());
      for (Object item : list) {
        String text = item == null ? null : String.valueOf(item).trim();
        if (text != null && !text.isEmpty()) {
          out.add(text);
        }
      }
      return out;
    }
    String text = String.valueOf(val).trim();
    return text.isEmpty() ? Collections.emptyList() : Collections.singletonList(text);
  }
  
  private static String jsonOf(Object val) {
    if (val == null) return null;
    if (val instanceof String) return (String) val;
    try {
      return MAPPER.writeValueAsString(val);
    } catch (Exception e) {
      return String.valueOf(val);
    }
  }
}
