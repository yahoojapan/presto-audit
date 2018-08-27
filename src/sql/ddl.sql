CREATE TABLE presto_audit_1 (
  queryId STRING,
  query STRING,
  uri STRING,
  state STRING,

  cpuTime DOUBLE,
  wallTime DOUBLE,
  queuedTime DOUBLE,
  peakMemoryBytes BIGINT,     --Deprecated
  peakUserMemoryBytes BIGINT,
  peakTotalNonRevocableMemoryBytes BIGINT,
  totalBytes BIGINT,
  totalRows BIGINT,
  completedSplits INTEGER,

  createTime STRING,          --Deprecated
  executionStartTime STRING,  --Deprecated
  endTime STRING,             --Decrecated

  createTimestamp DOUBLE,
  executionStartTimestamp DOUBLE,
  endTimestamp DOUBLE,

  errorCode INTEGER,
  errorName STRING,
  failureType STRING,
  failureMessage STRING,
  failuresJson STRING,

  remoteClientAddress STRING,
  clientUser STRING,
  userAgent STRING ,
  source STRING
  )
PARTITIONED BY (
  `ymd` string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
;
