package com.bancopel.dataflow;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface LoadBigQueryOptions extends GcpOptions {
  @Description("GCS input pattern, e.g. gs://bucket/raw/*.json")
  ValueProvider<String> getInput();
  void setInput(ValueProvider<String> value);

  @Description("project:dataset.table")
  ValueProvider<String> getBqTable();
  void setBqTable(ValueProvider<String> value);

  @Description("GCS temp location for BigQuery loads")
  ValueProvider<String> getBqTempLocation();
  void setBqTempLocation(ValueProvider<String> value);

  @Description("GCS prefix for parse failures")
  @Validation.Required
  ValueProvider<String> getDeadletter();
  void setDeadletter(ValueProvider<String> value);

  @Description("BigQuery table for errors. Format: project:dataset.table")
  @Validation.Required
  ValueProvider<String> getErrorTable();
  void setErrorTable(ValueProvider<String> value);

  @Description("Local output path for validation (TextIO). If set, BigQuery write is skipped.")
  ValueProvider<String> getLocalOutput();
  void setLocalOutput(ValueProvider<String> value);
}
