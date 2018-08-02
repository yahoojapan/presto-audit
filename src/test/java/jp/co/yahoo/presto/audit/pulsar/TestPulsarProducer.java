package jp.co.yahoo.presto.audit.pulsar;

import jp.co.yahoo.presto.audit.serializer.SerializedLog;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPulsarProducer
{
    @Test
    public void TestClientConfigurationBuilder()
            throws PulsarClientException.UnsupportedAuthenticationException
    {
        Map<String, String> authParams = new HashMap<>();
        ClientConfiguration mockConf = mock(ClientConfiguration.class);
        ClientConfiguration conf = new PulsarProducer.Builder()
                .setUseTLS(false)
                .setTrustCerts("/path/to/trusted_cert")
                .setAuthParams(authParams)
                .buildClientConfiguration(mockConf);

        assert(mockConf == conf);
        verify(mockConf).setUseTls(false);
        verify(mockConf).setTlsTrustCertsFilePath("/path/to/trusted_cert");
        verify(mockConf).setAuthentication(AuthenticationAthenz.class.getName(), authParams);
    }

    @Test
    public void TestProducerConfigurationBuilderDefault()
    {
        ProducerConfiguration mockConf = mock(ProducerConfiguration.class);
        ProducerConfiguration conf = new PulsarProducer.Builder()
                .buildProducerConfiguration(mockConf);

        assert(mockConf == conf);
        verify(mockConf).setSendTimeout(3, TimeUnit.SECONDS);
    }

    @Test
    public void TestProducerConfigurationBuilderConfig()
    {
        ProducerConfiguration mockConf = mock(ProducerConfiguration.class);
        ProducerConfiguration conf = new PulsarProducer.Builder()
                .setSendTimeout(30, TimeUnit.MILLISECONDS)
                .setCompressionType(CompressionType.ZLIB)
                .buildProducerConfiguration(mockConf);

        assert(mockConf == conf);
        verify(mockConf).setSendTimeout(30, TimeUnit.MILLISECONDS);
        verify(mockConf).setCompressionType(CompressionType.ZLIB);
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void TestBuilderFail()
            throws PulsarClientException
    {
        new PulsarProducer.Builder()
                .setURL("pulsar+ssl://pulsar.cluster.com:6651")
                .setTopic("persistent://namespace/global/test/topic1").build();
    }

    @Test
    public void TestPulsarProducer()
    {
        Producer producer = mock(Producer.class);
        PulsarProducer pulsarProducer = new PulsarProducer(producer);
        pulsarProducer.send(new SerializedLog("queryID", "{\"a\":\"b\"}"));
    }

    @Test
    public void TestPulsarProducerSendError()
            throws PulsarClientException
    {
        Producer producer = mock(Producer.class);
        when(producer.send(any(byte[].class)))
                .thenThrow(new PulsarClientException("Mock IO Exception"));
        PulsarProducer pulsarProducer = new PulsarProducer(producer);
        pulsarProducer.send(new SerializedLog("being_dropped_queryID", "{\"a\":\"b\"}"));
    }
}
