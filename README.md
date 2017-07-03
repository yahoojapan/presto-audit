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

## Requirement
* Presto 0.167 or later

# Build
## Patern 1: just build jar file, without RPM package
```bash
mvn -B -e clean package -Dmaven.test.skip=true -pl '!presto-audit-rpm,'
```

## Patern 2: build jar file, then RPM package
```bash
mvn -B -e clean package -Dbuild.no=2
```


# Usage
## install
```bash
sudo rpm -ivh presto-audit-plugin-0.167.1-2.el7.x86_64.rpm
```
## uninstall
```bash
sudo rpm -e presto-audit-plugin
```

## run
create a event-listener.properties file under /etc/presto/ .

eg.
/etc/presto/event-listener.properties
```bash
event-listener.name=presto-audit-log
event-listener.audit-log-path=/var/log/presto/
event-listener.audit-log-filename=presto-auditlog.log
```

