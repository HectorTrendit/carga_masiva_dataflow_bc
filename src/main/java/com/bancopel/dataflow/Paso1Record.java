package com.bancopel.dataflow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Paso1Record implements Serializable {
  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());
  private static final List<String> ARRAY_FIELDS = Arrays.asList(
      "rawLogIds",
      "rawLogs",
      "ioc_domain",
      "ioc_md5",
      "ioc_ip_v4",
      "ioc_fields",
      "ioc_types",
      "ioc_sources",
      "merged_msg_types",
      "added_permissions",
      "added_users",
      "allowed_data_actions",
      "allowed_ids",
      "allowed_permissions",
      "allowed_resources",
      "allowed_uris",
      "allowed_user_types",
      "allowed_users",
      "analyzers",
      "apps",
      "assigned_apps",
      "attributes",
      "block_public_acls",
      "block_public_policy",
      "categories",
      "category_ids",
      "collaborators",
      "denied_data_actions",
      "denied_permissions",
      "denied_resources",
      "denied_users",
      "email_attachments",
      "email_attachments_bytes",
      "email_dlp_policy_names",
      "email_recipients",
      "email_urls",
      "file_permissions",
      "asset_labels",
      "members",
      "mitre_labels",
      "modified_keys",
      "module_hash_names",
      "privileges",
      "profiles",
      "recipients",
      "removed_permissions",
      "removed_users",
      "reply_to",
      "role_permissions",
      "rule_usecases",
      "tags",
      "transistive_tags",
      "users",
      "invalidFields"
  );

  private final String id;
  private final String approxLogTime;
  private final String ingestTime;
  private final String ingestDate;
  private final String collectorTimestamp;
  private final String rowCreateTime;
  private final String tier;
  private final Long rawLogSize;
  private final Boolean parsed;
  private final List<String> rawLogIds;
  private final List<String> rawLogs;
  private final String metadataJson;
  private final String eventDebugJson;
  private final String payload;
  private final String sourceFile;

  public Paso1Record(
      String id,
      String approxLogTime,
      String ingestTime,
      String ingestDate,
      String collectorTimestamp,
      String rowCreateTime,
      String tier,
      Long rawLogSize,
      Boolean parsed,
      List<String> rawLogIds,
      List<String> rawLogs,
      String metadataJson,
      String eventDebugJson,
      String payload,
      String sourceFile) {
    this.id = id;
    this.approxLogTime = approxLogTime;
    this.ingestTime = ingestTime;
    this.ingestDate = ingestDate;
    this.collectorTimestamp = collectorTimestamp;
    this.rowCreateTime = rowCreateTime;
    this.tier = tier;
    this.rawLogSize = rawLogSize;
    this.parsed = parsed;
    this.rawLogIds = rawLogIds;
    this.rawLogs = rawLogs;
    this.metadataJson = metadataJson;
    this.eventDebugJson = eventDebugJson;
    this.payload = payload;
    this.sourceFile = sourceFile;
  }

  public String getId() {
    return id;
  }

  public String getApproxLogTime() {
    return approxLogTime;
  }

  public String getIngestTime() {
    return ingestTime;
  }

  public String getIngestDate() {
    return ingestDate;
  }

  public String getCollectorTimestamp() {
    return collectorTimestamp;
  }

  public String getRowCreateTime() {
    return rowCreateTime;
  }

  public String getTier() {
    return tier;
  }

  public Long getRawLogSize() {
    return rawLogSize;
  }

  public Boolean getParsed() {
    return parsed;
  }

  public List<String> getRawLogIds() {
    return rawLogIds;
  }

  public List<String> getRawLogs() {
    return rawLogs;
  }

  public String getMetadataJson() {
    return metadataJson;
  }

  public String getEventDebugJson() {
    return eventDebugJson;
  }

  public String getPayload() {
    return payload;
  }

  public String getSourceFile() {
    return sourceFile;
  }

  public TableRow toTableRow() {
    TableRow row = new TableRow();
    if (payload != null && !payload.isEmpty()) {
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = MAPPER.readValue(payload, Map.class);
        row.putAll(map);
      } catch (Exception ignored) {
      }
    }

    row.set("id", id);
    row.set("approxLogTime", approxLogTime);
    row.set("ingest_time", ingestTime);
    row.set("ingest_date", ingestDate);
    row.set("collector_timestamp", collectorTimestamp);
    row.set("row_create_time", rowCreateTime);
    row.set("tier", tier);
    row.set("raw_log_size", rawLogSize);
    row.set("parsed", parsed);
    row.set("rawLogIds", rawLogIds);
    row.set("rawLogs", rawLogs);

    row.set("customFieldsJSON", toJsonString(row.get("customFieldsJSON")));
    row.set("metadataFieldsJSON", toJsonString(row.get("metadataFieldsJSON")));
    row.set("event_debug_info", toJsonString(row.get("event_debug_info")));

    coerceArrayFields(row);

    row.set("payload", payload);
    row.set("source_file", sourceFile);
    return row;
  }

  private static void coerceArrayFields(TableRow row) {
    for (String field : ARRAY_FIELDS) {
      coerceArrayField(row, field);
    }
  }

  private static void coerceArrayField(TableRow row, String field) {
    Object value = row.get(field);
    if (!(value instanceof List)) {
      return;
    }
    List<?> list = (List<?>) value;
    List<String> out = new ArrayList<>(list.size());
    for (Object item : list) {
      out.add(item == null ? null : item.toString());
    }
    row.set(field, out);
  }

  private static String toJsonString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return (String) value;
    }
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception e) {
      return value.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Paso1Record that = (Paso1Record) o;
    return Objects.equals(id, that.id)
        && Objects.equals(approxLogTime, that.approxLogTime)
        && Objects.equals(ingestTime, that.ingestTime)
        && Objects.equals(ingestDate, that.ingestDate)
        && Objects.equals(collectorTimestamp, that.collectorTimestamp)
        && Objects.equals(rowCreateTime, that.rowCreateTime)
        && Objects.equals(tier, that.tier)
        && Objects.equals(rawLogSize, that.rawLogSize)
        && Objects.equals(parsed, that.parsed)
        && Objects.equals(rawLogIds, that.rawLogIds)
        && Objects.equals(rawLogs, that.rawLogs)
        && Objects.equals(metadataJson, that.metadataJson)
        && Objects.equals(eventDebugJson, that.eventDebugJson)
        && Objects.equals(payload, that.payload)
        && Objects.equals(sourceFile, that.sourceFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        approxLogTime,
        ingestTime,
        ingestDate,
        collectorTimestamp,
        rowCreateTime,
        tier,
        rawLogSize,
        parsed,
        rawLogIds,
        rawLogs,
        metadataJson,
        eventDebugJson,
        payload,
        sourceFile
    );
  }
}
