package com.bancopel.dataflow;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class ParseFileFnTest {

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void parsesNdjsonFile() throws Exception {
    String line1 = "{\"id\":\"1\",\"rawLogIds\":[\"a\"],\"rawLogs\":[\"l1\"]," +
        "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\"," +
        "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"tier\":\"Tier 4\"," +
        "\"raw_log_size\":\"207\",\"parsed\":false," +
        "\"metadataFieldsJSON\":{},\"event_debug_info\":{}}";
    String line2 = "{\"id\":\"2\",\"rawLogIds\":[\"b\"],\"rawLogs\":[\"l2\"]," +
        "\"approxLogTime\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"ingest_time\":\"2025-07-27 07:52:06.771 UTC\"," +
        "\"collector_timestamp\":\"2025-07-27 07:52:02.618 UTC\"," +
        "\"tier\":\"Tier 4\"," +
        "\"raw_log_size\":\"208\",\"parsed\":false," +
        "\"metadataFieldsJSON\":{},\"event_debug_info\":{}}";

    Path tempFile = Files.createTempFile("paso1", ".json");
    Files.write(tempFile, (line1 + "\n" + line2).getBytes(StandardCharsets.UTF_8));

    PCollectionTuple out = pipeline
        .apply(FileIO.match().filepattern(tempFile.toString()))
        .apply(FileIO.readMatches())
        .apply(ParDo.of(new ParseFileFn(TestTags.DEAD))
            .withOutputTags(TestTags.MAIN, TupleTagList.of(TestTags.DEAD)));

    PAssert.that(out.get(TestTags.MAIN)).satisfies(records -> {
      int count = 0;
      for (Paso1Record r : records) {
        count++;
      }
      assertEquals(2, count);
      return null;
    });
    PAssert.that(out.get(TestTags.DEAD)).empty();

    pipeline.run().waitUntilFinish();
  }
}
