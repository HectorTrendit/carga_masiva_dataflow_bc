package com.bancopel.dataflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;

public class ValidateRecordFnTest {

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void validatesOnlyIdAndPayload() {
    Paso1Record ok = record("1", "Tier 4", "2025-07-27", "{\"ok\":true}");
    Paso1Record missingPayload = record("2", "Tier 4", "2025-99-99", "");
    Paso1Record optionalFieldsStillPass = record("3", null, null, "{\"bad\":true}", -1L);

    PCollectionTuple out = pipeline
        .apply(Create.of(ok, missingPayload, optionalFieldsStillPass))
        .apply(ParDo.of(new ValidateRecordFn(TestTags.DEAD))
            .withOutputTags(TestTags.MAIN, TupleTagList.of(TestTags.DEAD)));

    PAssert.that(out.get(TestTags.MAIN)).satisfies(records -> {
      int count = 0;
      for (Paso1Record r : records) {
        count++;
      }
      assertEquals(2, count);
      return null;
    });

    PAssert.that(out.get(TestTags.DEAD)).satisfies(records -> {
      List<String> errors = new ArrayList<>();
      for (DeadletterRecord r : records) {
        errors.add(r.getError());
        assertEquals("validate", r.getStage());
      }
      assertEquals(1, errors.size());
      assertTrue(errors.contains("payload_missing"));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void rejectsMissingId() {
    Paso1Record missingId = record(null, "Tier 4", "2025-07-27", "{\"ok\":true}");

    PCollectionTuple out = pipeline
        .apply(Create.of(missingId))
        .apply(ParDo.of(new ValidateRecordFn(TestTags.DEAD))
            .withOutputTags(TestTags.MAIN, TupleTagList.of(TestTags.DEAD)));

    PAssert.that(out.get(TestTags.MAIN)).empty();

    PAssert.that(out.get(TestTags.DEAD)).satisfies(records -> {
      List<String> errors = new ArrayList<>();
      for (DeadletterRecord r : records) {
        errors.add(r.getError());
      }
      assertEquals(1, errors.size());
      assertTrue(errors.contains("id_missing"));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  private Paso1Record record(String id, String ingestDate, String payload) {
    return record(id, "Tier 4", ingestDate, payload);
  }

  private Paso1Record record(String id, String tier, String ingestDate, String payload) {
    return record(id, tier, ingestDate, payload, null);
  }

  private Paso1Record record(String id, String tier, String ingestDate, String payload, Long rawLogSize) {
    return new Paso1Record(
        id,
        null,
        null,
        ingestDate,
        null,
        null,
        tier,
        rawLogSize,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null,
        null,
        payload,
        "test-source"
    );
  }
}
