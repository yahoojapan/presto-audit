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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class AuditConfig
{
    private String auditLogPath;
    private String auditSimpleLogName;
    private String auditFullLogName;
    private String simpleLogTopic;
    private String fullLogTopic;
    private String url;
    private String trustCerts;
    private String logFilter;
    private String tenantDomain;
    private String tenantService;
    private String providerDomain;
    private String privateKey;
    private String athenzConfPath;
    private String principalHeader;
    private String roleHeader;
    private AuditLogFileWriter auditLogFileWriter = AuditLogFileWriter.getInstance();

    @NotNull
    public AuditLogFileWriter getAuditLogFileWriter()
    {
        return auditLogFileWriter;
    }

    public AuditConfig setAuditLogFileWriter(AuditLogFileWriter auditLogFileWriter)
    {
        this.auditLogFileWriter = auditLogFileWriter;
        return this;
    }

    @NotNull
    public String getAuditLogPath()
    {
        return auditLogPath;
    }

    @Config("event-listener.audit-log-path")
    @ConfigDescription("audit log path")
    public AuditConfig setAuditLogPath(String eventLogPath)
    {
        this.auditLogPath = eventLogPath;
        return this;
    }

    @NotNull
    public String getAuditSimpleLogName()
    {
        return auditSimpleLogName;
    }

    @Config("event-listener.audit-log-filename")
    @ConfigDescription("audit simple log file name")
    public AuditConfig setAuditSimpleLogName(String auditSimpleLogName)
    {
        this.auditSimpleLogName = auditSimpleLogName;
        return this;
    }

    @Nullable
    public String getAuditFullLogName()
    {
        return auditFullLogName;
    }

    @Config("event-listener.audit-log-full-filename")
    @ConfigDescription("audit full log file name")
    public AuditConfig setAuditFullLogName(String auditFullLogName)
    {
        this.auditFullLogName = auditFullLogName;
        return this;
    }

    @Nullable
    public String getSimpleTopic()
    {
        return simpleLogTopic;
    }

    @Config("event-listener.pulsar.simple-log-topic")
    @ConfigDescription("simple log topic")
    public AuditConfig setSimpleTopic(String simpleLogTopic)
    {
        this.simpleLogTopic = simpleLogTopic;
        return this;
    }

    @Nullable
    public String getFullTopic()
    {
        return fullLogTopic;
    }

    @Config("event-listener.pulsar.full-log-topic")
    @ConfigDescription("full log topic")
    public AuditConfig setFullTopic(String fullLogTopic)
    {
        this.fullLogTopic = fullLogTopic;
        return this;
    }

    @Nullable
    public String getPrivateKeyPath()
    {
        return privateKey;
    }

    @Config("event-listener.athenz.private-key-path")
    @ConfigDescription("private key path")
    public AuditConfig setPrivateKeyPath(String privateKey)
    {
        this.privateKey = privateKey;
        return this;
    }

    @Nullable
    public String getPulsarUrl()
    {
        return url;
    }

    @Config("event-listener.pulsar.pulsar-url")
    @ConfigDescription("pulsar url")
    public AuditConfig setPulsarUrl(String pulsarUrl)
    {
        this.url = pulsarUrl;
        return this;
    }

    @Nullable
    public String getTrustCertsPath()
    {
        return trustCerts;
    }

    @Config("event-listener.pulsar.pulsar-cert-path")
    @ConfigDescription("pulsar cert path")
    public AuditConfig setTrustCertsPath(String trustCertsPath)
    {
        this.trustCerts = trustCertsPath;
        return this;
    }

    @Nullable
    public String getLogFilter()
    {
        return logFilter;
    }

    @Config("event-listener.audit-log-full-filter")
    @ConfigDescription("audit log full filter")
    public AuditConfig setLogFilter(String auditLogFullFilter)
    {
        this.logFilter = auditLogFullFilter;
        return this;
    }

    @Nullable
    public String getTenantDomain()
    {
        return tenantDomain;
    }

    @Config("event-listener.athenz.tenant-domain")
    @ConfigDescription("tenant domain name")
    public AuditConfig setTenantDomain(String athenzTenantDomain)
    {
        this.tenantDomain = athenzTenantDomain;
        return this;
    }

    @Nullable
    public String getTenantService()
    {
        return tenantService;
    }

    @Config("event-listener.athenz.tenant-service")
    @ConfigDescription("tenant service name")
    public AuditConfig setTenantService(String athenzTenantService)
    {
        this.tenantService = athenzTenantService;
        return this;
    }

    @Nullable
    public String getProviderDomain()
    {
        return providerDomain;
    }

    @Config("event-listener.athenz.provider-domain")
    @ConfigDescription("athenz provider domain")
    public AuditConfig setProviderDomain(String athenzProviderDomain)
    {
        this.providerDomain = athenzProviderDomain;
        return this;
    }

    @Nullable
    public String getAthenzConfPath()
    {
        return athenzConfPath;
    }

    @Config("event-listener.athenz.config-path")
    @ConfigDescription("athenz config path")
    public AuditConfig setAthenzConfPath(String athenzConfigPath)
    {
        this.athenzConfPath = athenzConfigPath;
        return this;
    }

    @Nullable
    public String getPrincipalHeader()
    {
        return principalHeader;
    }

    @Config("event-listener.athenz.principal-header")
    @ConfigDescription("athenz principal header")
    public AuditConfig setPrincipalHeader(String athenzPrincipalHeader)
    {
        this.principalHeader = athenzPrincipalHeader;
        return this;
    }

    @Nullable
    public String getRoleHeader()
    {
        return roleHeader;
    }

    @Config("event-listener.athenz.role-header")
    @ConfigDescription("athenz role header")
    public AuditConfig setRoleHeader(String athenzRoleHeader)
    {
        this.roleHeader = athenzRoleHeader;
        return this;
    }
}
