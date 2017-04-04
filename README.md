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
#Presto Query Audit Log(log history)
Plugin for Presto to save queries and metrics into files.  

# Build
```bash
mvn clean package
```

# Usage
## install
```bash
sudo rpm -ivh presto-audit-rpm-0.157.1-t.0.7.x86_64.rpm
```

## 実行
下記プロパティファイルを作成し、/etc/presto/配下に置く。
ファイル名：event-listener.properties
```bash
event-listener.name=presto-audit-log
event-listener.auditlog_path=/var/log/presto/
event-listener.auditlog_filename=presto-auditlog.log
```

