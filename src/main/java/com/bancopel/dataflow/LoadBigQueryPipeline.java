package com.bancopel.dataflow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class LoadBigQueryPipeline {

  private static final String FIELD_ID = "id";
  private static final String FIELD_INGEST_DATE = "ingest_date";
  private static final String FIELD_TIER = "tier";

  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());

  private static TableFieldSchema field(String name, String type) {
    return new TableFieldSchema().setName(name).setType(type);
  }

  private static TableFieldSchema field(String name, String type, String mode) {
    return new TableFieldSchema().setName(name).setType(type).setMode(mode);
  }

  private static TableSchema schema() {
    return new TableSchema().setFields(Arrays.asList(
        field("id", "STRING"),
        field("approxLogTime", "TIMESTAMP"),
        field("ingest_time", "TIMESTAMP"),
        field("ingest_date", "DATE"),
        field("collector_timestamp", "TIMESTAMP"),
        field("row_create_time", "TIMESTAMP"),
        field("tier", "STRING"),
        field("raw_log_size", "INT64"),
        field("parsed", "BOOL"),
        field("rawLogIds", "STRING", "REPEATED"),
        field("rawLogs", "STRING", "REPEATED"),
        field("customFieldsJSON", "STRING"),
        field("metadataFieldsJSON", "STRING"),
        field("event_debug_info", "STRING"),
        field("ioc_domain", "STRING", "REPEATED"),
        field("ioc_md5", "STRING", "REPEATED"),
        field("ioc_ip_v4", "STRING", "REPEATED"),
        field("ioc_fields", "STRING", "REPEATED"),
        field("ioc_types", "STRING", "REPEATED"),
        field("ioc_sources", "STRING", "REPEATED"),
        field("merged_msg_types", "STRING", "REPEATED"),
        field("parser_version", "STRING"),
        field("added_permissions", "STRING", "REPEATED"),
        field("added_users", "STRING", "REPEATED"),
        field("allowed_data_actions", "STRING", "REPEATED"),
        field("allowed_ids", "STRING", "REPEATED"),
        field("allowed_permissions", "STRING", "REPEATED"),
        field("allowed_resources", "STRING", "REPEATED"),
        field("allowed_uris", "STRING", "REPEATED"),
        field("allowed_user_types", "STRING", "REPEATED"),
        field("allowed_users", "STRING", "REPEATED"),
        field("analyzers", "STRING", "REPEATED"),
        field("apps", "STRING", "REPEATED"),
        field("assigned_apps", "STRING", "REPEATED"),
        field("attributes", "STRING", "REPEATED"),
        field("block_public_acls", "STRING", "REPEATED"),
        field("block_public_policy", "STRING", "REPEATED"),
        field("categories", "STRING", "REPEATED"),
        field("category_ids", "STRING", "REPEATED"),
        field("collaborators", "STRING", "REPEATED"),
        field("denied_data_actions", "STRING", "REPEATED"),
        field("denied_permissions", "STRING", "REPEATED"),
        field("denied_resources", "STRING", "REPEATED"),
        field("denied_users", "STRING", "REPEATED"),
        field("email_attachments", "STRING", "REPEATED"),
        field("email_attachments_bytes", "STRING", "REPEATED"),
        field("email_dlp_policy_names", "STRING", "REPEATED"),
        field("email_recipients", "STRING", "REPEATED"),
        field("email_urls", "STRING", "REPEATED"),
        field("file_permissions", "STRING", "REPEATED"),
        field("asset_labels", "STRING", "REPEATED"),
        field("members", "STRING", "REPEATED"),
        field("mitre_labels", "STRING", "REPEATED"),
        field("modified_keys", "STRING", "REPEATED"),
        field("module_hash_names", "STRING", "REPEATED"),
        field("privileges", "STRING", "REPEATED"),
        field("profiles", "STRING", "REPEATED"),
        field("recipients", "STRING", "REPEATED"),
        field("removed_permissions", "STRING", "REPEATED"),
        field("removed_users", "STRING", "REPEATED"),
        field("reply_to", "STRING", "REPEATED"),
        field("role_permissions", "STRING", "REPEATED"),
        field("rule_usecases", "STRING", "REPEATED"),
        field("tags", "STRING", "REPEATED"),
        field("transistive_tags", "STRING", "REPEATED"),
        field("users", "STRING", "REPEATED"),
        field("invalidFields", "STRING", "REPEATED"),
        field("payload", "STRING"),
        field("source_file", "STRING")
    ));
  }

  private static TableSchema errorSchema() {
    return new TableSchema().setFields(Arrays.asList(
        field("source_file", "STRING"),
        field("record_index", "INT64"),
        field("error", "STRING"),
        field("stage", "STRING"),
        field("timestamp", "TIMESTAMP"),
        field("payload", "STRING")
    ));
  }

  private static List<String> expectedFieldOrder() {
    List<TableFieldSchema> fields = schema().getFields();
    List<String> names = new ArrayList<>(fields.size());
    for (TableFieldSchema field : fields) {
      names.add(field.getName());
    }
    return names;
  }

  private static void validateSchema(
      TableSchema schema, TimePartitioning timePartitioning, Clustering clustering) {
    if (schema == null || schema.getFields() == null) {
      throw new IllegalStateException("BigQuery schema is required");
    }

    Set<String> schemaFields = new LinkedHashSet<>();
    Map<String, String> schemaTypes = new HashMap<>();
    for (TableFieldSchema field : schema.getFields()) {
      schemaFields.add(field.getName());
      schemaTypes.put(field.getName(), field.getType());
    }

    Set<String> expectedFields = new LinkedHashSet<>(expectedFieldOrder());
    Set<String> missing = new LinkedHashSet<>(expectedFields);
    missing.removeAll(schemaFields);
    Set<String> extra = new LinkedHashSet<>(schemaFields);
    extra.removeAll(expectedFields);

    if (!missing.isEmpty() || !extra.isEmpty()) {
      throw new IllegalStateException(
          "Schema mismatch. Missing: " + missing + " Extra: " + extra);
    }

    if (timePartitioning != null && timePartitioning.getField() != null) {
      String field = timePartitioning.getField();
      if (!schemaFields.contains(field)) {
        throw new IllegalStateException("Partition field not in schema: " + field);
      }
      String fieldType = schemaTypes.get(field);
      if (!"DATE".equals(fieldType)) {
        throw new IllegalStateException(
            "Partition field must be DATE. Field: " + field + " Type: " + fieldType);
      }
    }

    if (clustering != null && clustering.getFields() != null) {
      for (String field : clustering.getFields()) {
        if (!schemaFields.contains(field)) {
          throw new IllegalStateException("Clustering field not in schema: " + field);
        }
      }
    }
  }

  private static TimePartitioning buildTimePartitioning() {
    return new TimePartitioning().setType("DAY").setField(FIELD_INGEST_DATE);
  }

  private static Clustering buildClustering() {
    return new Clustering().setFields(Arrays.asList(FIELD_TIER, FIELD_ID));
  }

  private static boolean useLocalOutput(LoadBigQueryOptions options) {
    if (options.getLocalOutput() == null) {
      return false;
    }
    if (!options.getLocalOutput().isAccessible()) {
      return false;
    }
    String value = options.getLocalOutput().get();
    return value != null && !value.isEmpty();
  }

  private static void buildPipeline(Pipeline p, LoadBigQueryOptions options) {
    boolean localOutput = useLocalOutput(options);
    TableSchema bqSchema = null;
    TimePartitioning timePartitioning = null;
    Clustering clustering = null;
    if (!localOutput) {
      bqSchema = schema();
      timePartitioning = buildTimePartitioning();
      clustering = buildClustering();
      validateSchema(bqSchema, timePartitioning, clustering);
    }

    ParseAndValidateTransform parseAndValidate = new ParseAndValidateTransform();
    PCollectionTuple parsed = p
        .apply("MatchFiles", FileIO.match().filepattern(options.getInput()))
        .apply("ReadMatches", FileIO.readMatches())
        .apply("ParseAndValidate", parseAndValidate);

    PCollection<Paso1Record> main = parsed.get(parseAndValidate.getMainTag());
    PCollection<DeadletterRecord> deadletters = parsed.get(parseAndValidate.getDeadTag());

    TupleTag<Paso1Record> schemaMainTag = new TupleTag<Paso1Record>() {};
    TupleTag<DeadletterRecord> schemaDeadTag = new TupleTag<DeadletterRecord>() {};
    PCollectionTuple schemaChecked = main.apply("CheckSchema",
        ParDo.of(new CheckSchemaFn(schemaDeadTag, expectedFieldOrder()))
            .withOutputTags(schemaMainTag, TupleTagList.of(schemaDeadTag)));

    PCollection<Paso1Record> clean = schemaChecked.get(schemaMainTag);
    deadletters = PCollectionList.of(deadletters)
        .and(schemaChecked.get(schemaDeadTag))
        .apply("MergeAllDeadletters", Flatten.pCollections());

    if (localOutput) {
      clean
          .apply("LocalToJson", MapElements.into(TypeDescriptors.strings())
              .via((Paso1Record r) -> {
                try {
                  return MAPPER.writeValueAsString(r);
                } catch (Exception e) {
                  return "{\"error\":\"local_serialization_failed\"}";
                }
              }))
          .apply("WriteLocalOutput", TextIO.write()
              .to(options.getLocalOutput())
              .withSuffix(".jsonl"));
    } else {
      clean
          .apply("ToTableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
              .via(Paso1Record::toTableRow))
          .apply("WriteBQ", BigQueryIO.writeTableRows()
              .to(options.getBqTable())
              .withSchema(bqSchema)
              .withMethod(Method.FILE_LOADS)
              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
              .withCustomGcsTempLocation(options.getBqTempLocation())
              .withTimePartitioning(timePartitioning)
              .withClustering(clustering));
    }

    deadletters
        .apply("DeadletterToTableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
            .via((DeadletterRecord r) -> new TableRow()
                .set("source_file", r.getSourceFile())
                .set("record_index", r.getRecordIndex())
                .set("error", r.getError())
                .set("stage", r.getStage())
                .set("timestamp", r.getTimestamp())
                .set("payload", r.getPayload())))
        .apply("WriteDeadletterBQ", BigQueryIO.writeTableRows()
            .to(options.getErrorTable())
            .withSchema(errorSchema())
            .withMethod(Method.FILE_LOADS)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCustomGcsTempLocation(options.getBqTempLocation()));

    if (options.getDeadletter() != null && options.getDeadletter().get() != null && !options.getDeadletter().get().isEmpty()) {
      deadletters
          .apply("DeadletterToJson", MapElements.into(TypeDescriptors.strings())
              .via((DeadletterRecord r) -> {
                try {
                  return MAPPER.writeValueAsString(r);
                } catch (Exception e) {
                  return "{\"error\":\"deadletter_serialization_failed\"}";
                }
              }))
          .apply("WriteDeadletter", TextIO.write()
              .to(options.getDeadletter())
              .withSuffix(".jsonl"));
    }
  }

  public static void main(String[] args) {
    LoadBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(LoadBigQueryOptions.class);
    Pipeline p = Pipeline.create(options);
    buildPipeline(p, options);
    p.run();
  }
}
