<!--
{% comment %}
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
{% endcomment %}
-->
# Presto Query Audit Log(log history)
Plugin for Presto to save queries and metrics into files.

[![Build Status](https://travis-ci.org/yahoojapan/presto-audit.svg?branch=master)](https://travis-ci.org/yahoojapan/presto-audit)

## Requirement
* Presto 0.167 or later

# Build

```bash
% ./mvnw clean compile package
```

## packaging - rpm

```bash
% ./mvnw clean compile package rpm:rpm
```

# Usage

## install

Copy jar file to target directory.

```bash
% export PLUGIN_VER=1.0
% cp target/presto-audit-plugin-${PLUGIN_VER}-SNAPSHOT-all.jar /usr/lib/presto/lib/plugin/yj-audit/
```

## run
create a event-listener.properties file under /etc/presto/ .

eg.
/etc/presto/event-listener.properties
```text
event-listener.name=presto-audit-log
event-listener.audit-log-path=/var/log/presto/
event-listener.audit-log-filename=presto-auditlog.log
```

## full log
To enable full log, add the following line to the file
```text
event-listener.audit-log-full-filename=presto-auditlog-full.log
```

To add a filter for full log, add the following line to the file.
The filter should be a regex. If the filter match with `--source` of a query, this query will not be
logged in full log.
```text
# Format:
event-listener.audit-log-full-filter=<regex>

# Example:
event-listener.audit-log-full-filter=SRE_SYSTEM
event-listener.audit-log-full-filter=(SRE_SYSTEM|Presto-team)
```

## Send message to Pulsar broker
Send the log to Pulsar broker immediately after a query is completed.
Config:
```text
event-listener.pulsar.simple-log-topic=persistent://namespace/global/test/topic1
event-listener.pulsar.full-log-topic=persistent://namespace/global/test/topic2
event-listener.pulsar.pulsar-url=pulsar+ssl://pulsar.cluster.com:6651
event-listener.pulsar.pulsar-cert-path=/etc/pki/tls/certs/ca-bundle.crt
event-listener.pulsar.use-TLS=true  #Optional
event-listener.athenz.private-key-path=/etc/presto/athenz/private.key
event-listener.athenz.tenant-domain=tenant.pulsar.tenant
event-listener.athenz.tenant-service=mq
event-listener.athenz.provider-domain=provider.plusar.tenant
event-listener.athenz.config-path=/usr/local/etc/pulsar-athenz-config/athenz.conf
event-listener.athenz.principal-header=Athenz-Principal-Auth
event-listener.athenz.role-header=Athenz-Role-Auth
```

## Analyze SQL samples
Table DDL can be found in src/sql/ddl.sql
```sql
SELECT
    date_parse(executionStartTime, '%Y%m%d%H%i%S.%f') AS EXEC_START,
    state,
    date_diff('millisecond', date_parse(executionStartTime, '%Y%m%d%H%i%S.%f'), date_parse(endTime, '%Y%m%d%H%i%S.%f')) AS PRESTO_EXEC_TIME_MS
FROM
    presto_audit
WHERE
    ymd = '20170912' AND createTime > '20170912233000.572'
ORDER BY
    createTime;
```
