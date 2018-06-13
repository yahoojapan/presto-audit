package jp.co.yahoo.presto.audit.serializer;

public class SerializedLog
{
    private String queryId;
    private String serializedLog;

    public SerializedLog(String queryId, String serializedLog)
    {
        this.queryId = queryId;
        this.serializedLog = serializedLog;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getSerializedLog()
    {
        return serializedLog;
    }
}
