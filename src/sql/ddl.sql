CREATE TABLE presto_audit_1 (
  queryId STRING,
  query STRING,
  uri STRING,
  state STRING,

  cpuTime DOUBLE,
  wallTime DOUBLE,
  queuedTime DOUBLE,
  peakMemoryBytes BIGINT,
  totalBytes BIGINT,
  totalRows BIGINT,

  createTime STRING,
  executeStartTime STRING,
  endTime STRING,

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
