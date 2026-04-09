package com.bancopel.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class ValidateRecordFnTest {

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void validatesPayloadAndIngestDate() {
    Paso1Record ok = record("1", "2025-07-27", "{\"ok\":true}");
    Paso1Record missingPayload = record("2", "2025-07-27", "");
    Paso1Record invalidDate = record("3", "2025-99-99", "{\"bad\":true}");

    PCollectionTuple out = pipeline
        .apply(Create.of(ok, missingPayload, invalidDate))
        .apply(ParDo.of(new ValidateRecordFn(TestTags.DEAD))
            .withOutputTags(TestTags.MAIN, TupleTagList.of(TestTags.DEAD)));

    PAssert.that(out.get(TestTags.MAIN)).satisfies(records -> {
      int count = 0;
      for (Paso1Record r : records) {
        count++;
      }
      assertEquals(1, count);
      return null;
    });

    PAssert.that(out.get(TestTags.DEAD)).satisfies(records -> {
      List<String> errors = new ArrayList<>();
      for (DeadletterRecord r : records) {
        errors.add(r.getError());
        assertEquals("validate", r.getStage());
      }
      assertEquals(2, errors.size());
      assertTrue(errors.contains("payload_missing"));
      assertTrue(errors.contains("ingest_date_invalid"));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  private Paso1Record record(String id, String ingestDate, String payload) {
    return new Paso1Record(
        id,
        null,
        null,
        ingestDate,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null,
        payload,
        "test-source"
    );
  }
}
