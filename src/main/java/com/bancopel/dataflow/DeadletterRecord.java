package com.bancopel.dataflow;

import java.io.Serializable;

public class DeadletterRecord implements Serializable {
  private final String sourceFile;
  private final Long recordIndex;
  private final String error;
  private final String stage;
  private final String timestamp;
  private final String payload;

  public DeadletterRecord(
      String sourceFile,
      Long recordIndex,
      String error,
      String stage,
      String timestamp,
      String payload) {
    this.sourceFile = sourceFile;
    this.recordIndex = recordIndex;
    this.error = error;
    this.stage = stage;
    this.timestamp = timestamp;
    this.payload = payload;
  }

  public String getSourceFile() {
    return sourceFile;
  }

  public Long getRecordIndex() {
    return recordIndex;
  }

  public String getError() {
    return error;
  }

  public String getStage() {
    return stage;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getPayload() {
    return payload;
  }
}
