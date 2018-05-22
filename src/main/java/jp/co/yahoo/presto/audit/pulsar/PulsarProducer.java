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

import io.airlift.log.Logger;
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
    private PulsarClient pulsarClient;
    private Producer producer;

    public void initProducer(String topic, String url, String trustCerts, Map<String, String> authParams)
            throws PulsarClientException
    {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setUseTls(true);
        conf.setTlsTrustCertsFilePath(trustCerts);
        conf.setAuthentication(AuthenticationAthenz.class.getName(), authParams);

        pulsarClient = PulsarClient.create(url, conf);

        ProducerConfiguration prodConf = new ProducerConfiguration();
        prodConf.setSendTimeout(3, TimeUnit.SECONDS);
        producer = pulsarClient.createProducer(topic, prodConf);
    }

    public void send(String message)
    {
        try {
            producer.send(message.getBytes());
        }
        catch (PulsarClientException e) {
            log.error(e.getMessage());
        }
    }

    public void close()
            throws PulsarClientException
    {
        pulsarClient.close();
    }
}
