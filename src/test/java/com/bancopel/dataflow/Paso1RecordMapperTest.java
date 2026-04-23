package com.bancopel.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class Paso1RecordMapperTest {

  @Test
  public void mapsBasicFields() throws Exception {
    String json = "{" +
        "\"id\":\"1\"," +
        "\"rawLogIds\":[\"a\"]," +
        "\"rawLogs\":[\"log\"]," +
        "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\"," +
        "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"tier\":\"Tier 4\"," +
        "\"raw_log_size\":\"207\"," +
        "\"parsed\":false," +
        "\"metadataFieldsJSON\":{}," +
        "\"event_debug_info\":{}" +
        "}";

    Paso1Record record = Paso1RecordMapper.fromJson(json, "gs://bucket/file.json");

    assertEquals("1", record.getId());
    assertEquals("Tier 4", record.getTier());
    assertEquals(Long.valueOf(207), record.getRawLogSize());
    assertEquals("2025-07-27", record.getIngestDate());
    assertEquals("gs://bucket/file.json", record.getSourceFile());
    assertNotNull(record.getPayload());
    assertEquals(1, record.getRawLogIds().size());
    assertEquals(1, record.getRawLogs().size());
  }

  @Test
  public void mapsJsonNodeUsingTreeConversion() throws Exception {
    String json = "{" +
        "\"id\":\"1\"," +
        "\"rawLogIds\":[\"a\"]," +
        "\"rawLogs\":[\"log\"]," +
        "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\"," +
        "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"tier\":\"Tier 4\"," +
        "\"raw_log_size\":\"207\"," +
        "\"parsed\":false," +
        "\"metadataFieldsJSON\":{}," +
        "\"event_debug_info\":{}" +
        "}";

    JsonNode node = new ObjectMapper().readTree(json);
    Paso1Record record = Paso1RecordMapper.fromJson(node, "gs://bucket/file.json");

    assertEquals("1", record.getId());
    assertEquals("2025-07-27", record.getIngestDate());
    assertEquals("gs://bucket/file.json", record.getSourceFile());
  }
}
