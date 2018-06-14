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
package jp.co.yahoo.presto.audit.pulsar;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import jp.co.yahoo.presto.audit.serializer.SerializedLog;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PulsarProducer
{
    private static final Logger log = Logger.get(PulsarProducer.class);
    private Producer producer;

    @VisibleForTesting
    public PulsarProducer(Producer producer)
    {
        this.producer = producer;
    }

    public void send(SerializedLog message)
    {
        try {
            producer.send(message.getSerializedLog().getBytes());
        }
        catch (PulsarClientException e) {
            log.error("Failed to send message to Pulsar broker. " + e);
            log.error("Dropped queryID: " + message.getQueryId());
        }
    }

    public static class Builder
    {
        private String topic;
        private String url;
        private String trustCerts;
        private Map<String, String> authParams;
        private boolean useTLS;
        private int sendTimeout = 3;
        private TimeUnit sendTimeoutUnit = TimeUnit.SECONDS;

        public Builder setTopic(String topic)
        {
            this.topic = topic;
            return this;
        }

        public Builder setURL(String url)
        {
            this.url = url;
            return this;
        }

        public Builder setTrustCerts(String trustCerts)
        {
            this.trustCerts = trustCerts;
            return this;
        }

        public Builder setAuthParams(Map<String, String> authParams)
        {
            this.authParams = authParams;
            return this;
        }

        public Builder setUseTLS(boolean useTLS)
        {
            this.useTLS = useTLS;
            return this;
        }

        public Builder setSendTimeout(int sendTimeout, TimeUnit timeUnit)
        {
            this.sendTimeout = sendTimeout;
            this.sendTimeoutUnit = timeUnit;
            return this;
        }

        @VisibleForTesting
        ClientConfiguration buildClientConfiguration(ClientConfiguration conf)
                throws PulsarClientException.UnsupportedAuthenticationException
        {
            // Config PulsarClient
            conf.setUseTls(useTLS);
            conf.setTlsTrustCertsFilePath(trustCerts);
            conf.setAuthentication(AuthenticationAthenz.class.getName(), authParams);
            return conf;
        }

        @VisibleForTesting
        ProducerConfiguration buildProducerConfiguration(ProducerConfiguration prodConf)
        {
            prodConf.setSendTimeout(sendTimeout, sendTimeoutUnit);
            return prodConf;
        }

        public PulsarProducer build()
                throws PulsarClientException
        {
            ClientConfiguration conf = buildClientConfiguration(new ClientConfiguration());
            ProducerConfiguration prodConf = buildProducerConfiguration(new ProducerConfiguration());
            PulsarClient pulsarClient = PulsarClient.create(url, conf);
            Producer producer = pulsarClient.createProducer(topic, prodConf);
            return new PulsarProducer(producer);
        }
    }
}
