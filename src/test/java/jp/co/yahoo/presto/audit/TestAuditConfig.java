/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.presto.audit;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAuditConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(AuditConfig.class)
                .setAuditLogPath(null)
                .setAuditSimpleLogName(null)
                .setAuditFullLogName(null)
                .setSimpleTopic(null)
                .setFullTopic(null)
                .setPrivateKeyPath(null)
                .setPulsarUrl(null)
                .setTrustCertsPath(null)
                .setLogFilter(null)
                .setTenantDomain(null)
                .setTenantService(null)
                .setProviderDomain(null)
                .setAthenzConfPath(null)
                .setPrincipalHeader(null)
                .setRoleHeader(null));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("event-listener.audit-log-path", "/var/log/presto/")
                .put("event-listener.audit-log-filename", "presto-audit.log")
                .put("event-listener.audit-log-full-filename", "presto-auditlog-full.log")
                .put("event-listener.audit-log-full-filter", "DROP")
                .put("event-listener.pulsar.simple-log-topic", "persistent://namespace/global/test/topic1")
                .put("event-listener.pulsar.full-log-topic", "persistent://namespace/global/test/topic2")
                .put("event-listener.pulsar.pulsar-url", "pulsar+ssl://pulsar.cluster.com:6651")
                .put("event-listener.pulsar.pulsar-cert-path", "/etc/pki/tls/certs/ca-bundle.crt")
                .put("event-listener.athenz.private-key-path", "/etc/presto/athenz/private.key")
                .put("event-listener.athenz.tenant-domain", "tenant.pulsar.tenant")
                .put("event-listener.athenz.tenant-service", "mq")
                .put("event-listener.athenz.provider-domain", "provider.plusar.tenant")
                .put("event-listener.athenz.config-path", "/usr/local/etc/pulsar-athenz-config/athenz.conf")
                .put("event-listener.athenz.principal-header", "Athenz-Principal-Auth")
                .put("event-listener.athenz.role-header", "Athenz-Principal-Auth")
                .build();

        AuditConfig expected = new AuditConfig()
                .setAuditLogPath("/var/log/presto/")
                .setAuditSimpleLogName("presto-audit.log")
                .setAuditFullLogName("presto-auditlog-full.log")
                .setSimpleTopic("persistent://namespace/global/test/topic1")
                .setFullTopic("persistent://namespace/global/test/topic2")
                .setPrivateKeyPath("/etc/presto/athenz/private.key")
                .setPulsarUrl("pulsar+ssl://pulsar.cluster.com:6651")
                .setTrustCertsPath("/etc/pki/tls/certs/ca-bundle.crt")
                .setLogFilter("DROP")
                .setTenantDomain("tenant.pulsar.tenant")
                .setTenantService("mq")
                .setProviderDomain("provider.plusar.tenant")
                .setAthenzConfPath("/usr/local/etc/pulsar-athenz-config/athenz.conf")
                .setPrincipalHeader("Athenz-Principal-Auth")
                .setRoleHeader("Athenz-Principal-Auth");

        assertFullMapping(properties, expected);
    }
}
