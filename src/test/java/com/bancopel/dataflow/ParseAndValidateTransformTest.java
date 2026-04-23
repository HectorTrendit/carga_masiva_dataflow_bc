package com.bancopel.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

public class ParseAndValidateTransformTest {

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void parsesFileAndCapturesDeadletters() throws Exception {
    String validLine = "{\"id\":\"1\",\"rawLogIds\":[\"a\"],\"rawLogs\":[\"log\"],"
        + "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\","
        + "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\","
        + "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\","
        + "\"tier\":\"Tier 4\","
        + "\"raw_log_size\":\"207\",\"parsed\":false,"
        + "\"metadataFieldsJSON\":{},\"event_debug_info\":{}}";
    String invalidLine = "{not-json";

    Path tempFile = Files.createTempFile("paso1-flow", ".json");
    Files.write(tempFile, (validLine + "\n" + invalidLine).getBytes(StandardCharsets.UTF_8));

    ParseAndValidateTransform transform = new ParseAndValidateTransform();
    PCollectionTuple out = pipeline
        .apply(FileIO.match().filepattern(tempFile.toString()))
        .apply(FileIO.readMatches())
        .apply(transform);

    PAssert.that(out.get(transform.getMainTag())).satisfies(records -> {
      int count = 0;
      for (Paso1Record record : records) {
        count++;
        assertEquals("1", record.getId());
        assertTrue(record.getSourceFile() != null && !record.getSourceFile().isEmpty());
      }
      assertEquals(1, count);
      return null;
    });

    PAssert.that(out.get(transform.getDeadTag())).satisfies(records -> {
      int count = 0;
      boolean hasParseStage = false;
      for (DeadletterRecord record : records) {
        count++;
        hasParseStage = hasParseStage || "parse".equals(record.getStage());
        assertTrue(record.getError() != null && !record.getError().isEmpty());
      }
      assertEquals(1, count);
      assertTrue(hasParseStage);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void parsesFileWithoutTrailingNewline() throws Exception {
    String validLine = "{\"id\":\"1\",\"rawLogIds\":[\"a\"],\"rawLogs\":[\"log\"],"
        + "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\","
        + "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\","
        + "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\","
        + "\"tier\":\"Tier 4\","
        + "\"raw_log_size\":\"207\",\"parsed\":false,"
        + "\"metadataFieldsJSON\":{},\"event_debug_info\":{}}";

    Path tempFile = Files.createTempFile("paso1-flow-no-newline", ".json");
    Files.write(tempFile, validLine.getBytes(StandardCharsets.UTF_8));

    ParseAndValidateTransform transform = new ParseAndValidateTransform();
    PCollectionTuple out = pipeline
        .apply(FileIO.match().filepattern(tempFile.toString()))
        .apply(FileIO.readMatches())
        .apply(transform);

    PAssert.that(out.get(transform.getMainTag())).satisfies(records -> {
      int count = 0;
      for (Paso1Record record : records) {
        count++;
        assertEquals("1", record.getId());
      }
      assertEquals(1, count);
      return null;
    });

    PAssert.that(out.get(transform.getDeadTag())).empty();

    pipeline.run().waitUntilFinish();
  }
}
