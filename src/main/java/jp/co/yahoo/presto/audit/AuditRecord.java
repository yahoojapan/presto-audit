/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.presto.audit;

public class AuditRecord
{
    private String queryId;
    private String query;
    private String uri;
    private String state;

    private Double cpuTime;
    private Double wallTime;
    private Double queuedTime;
    private Long peakMemoryBytes;
    private Long totalBytes;
    private Long totalRows;
    private int completedSplits;

    @Deprecated private String createTime;
    @Deprecated private String executionStartTime;
    @Deprecated private String endTime;

    private Double createTimestamp;
    private Double executionStartTimestamp;
    private Double endTimestamp;

    private int errorCode;
    private String errorName;
    private String failureType;
    private String failureMessage;
    private String failuresJson;

    private String remoteClientAddress;
    private String clientUser;
    private String userAgent;
    private String source;

    public String getQueryId()
    {
        return queryId;
    }

    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
    }

    public String getQuery()
    {
        return query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public String getState()
    {
        return state;
    }

    public void setState(String state)
    {
        this.state = state;
    }

    public Double getCpuTime()
    {
        return cpuTime;
    }

    public void setCpuTime(Double cpuTime)
    {
        this.cpuTime = cpuTime;
    }

    public Double getWallTime()
    {
        return wallTime;
    }

    public void setWallTime(Double wallTime)
    {
        this.wallTime = wallTime;
    }

    public Double getQueuedTime()
    {
        return queuedTime;
    }

    public void setQueuedTime(Double queuedTime)
    {
        this.queuedTime = queuedTime;
    }

    public Long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public void setPeakMemoryBytes(Long peakMemoryBytes)
    {
        this.peakMemoryBytes = peakMemoryBytes;
    }

    public Long getTotalBytes()
    {
        return totalBytes;
    }

    public void setTotalBytes(Long totalBytes)
    {
        this.totalBytes = totalBytes;
    }

    public Long getTotalRows()
    {
        return totalRows;
    }

    public void setTotalRows(Long totalRows)
    {
        this.totalRows = totalRows;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public void setCompletedSplits(int completedSplits)
    {
        this.completedSplits = completedSplits;
    }

    public int getErrorCode()
    {
        return errorCode;
    }

    public void setErrorCode(int errorCode)
    {
        this.errorCode = errorCode;
    }

    public String getErrorName()
    {
        return errorName;
    }

    public void setErrorName(String errorName)
    {
        this.errorName = errorName;
    }

    public String getFailureType()
    {
        return failureType;
    }

    public void setFailureType(String failureType)
    {
        this.failureType = failureType;
    }

    public String getFailureMessage()
    {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage)
    {
        this.failureMessage = failureMessage;
    }

    public String getFailuresJson()
    {
        return failuresJson;
    }

    public void setFailuresJson(String failuresJson)
    {
        this.failuresJson = failuresJson;
    }

    public String getCreateTime()
    {
        return createTime;
    }

    public void setCreateTime(String createTime)
    {
        this.createTime = createTime;
    }

    public String getExecutionStartTime()
    {
        return executionStartTime;
    }

    public void setExecutionStartTime(String executionStartTime)
    {
        this.executionStartTime = executionStartTime;
    }

    public String getEndTime()
    {
        return endTime;
    }

    public void setEndTime(String endTime)
    {
        this.endTime = endTime;
    }

    public String getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    public void setRemoteClientAddress(String remoteClientAddress)
    {
        this.remoteClientAddress = remoteClientAddress;
    }

    public String getClientUser()
    {
        return clientUser;
    }

    public void setClientUser(String clientUser)
    {
        this.clientUser = clientUser;
    }

    public String getUserAgent()
    {
        return userAgent;
    }

    public void setUserAgent(String userAgent)
    {
        this.userAgent = userAgent;
    }

    public String getSource()
    {
        return source;
    }

    public void setSource(String source)
    {
        this.source = source;
    }

    public Double getCreateTimestamp()
    {
        return createTimestamp;
    }

    public void setCreateTimestamp(Double createTimestamp)
    {
        this.createTimestamp = createTimestamp;
    }

    public Double getExecutionStartTimestamp()
    {
        return executionStartTimestamp;
    }

    public void setExecutionStartTimestamp(Double executionStartTimestamp)
    {
        this.executionStartTimestamp = executionStartTimestamp;
    }

    public Double getEndTimestamp()
    {
        return endTimestamp;
    }

    public void setEndTimestamp(Double endTimestamp)
    {
        this.endTimestamp = endTimestamp;
    }
}
