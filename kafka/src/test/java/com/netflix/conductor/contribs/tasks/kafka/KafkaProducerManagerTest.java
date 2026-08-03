/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.tasks.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KafkaProducerManagerTest {

    @Test
    public void testRequestTimeoutSetFromDefault() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(100),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals(props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "100");
    }

    @Test
    public void testRequestTimeoutSetFromInput() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(100),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        input.setRequestTimeoutMs(200);
        Properties props = manager.getProducerProperties(input);
        assertEquals(props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "200");
    }

    @Test
    public void testRequestTimeoutSetFromConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals(props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "150");
    }

    @SuppressWarnings("rawtypes")
    @Test(expected = RuntimeException.class)
    public void testExecutionException() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Producer producer = manager.getProducer(input);
        assertNotNull(producer);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testCacheInvalidation() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150), Duration.ofMillis(500), 0, Duration.ofMillis(0));
        KafkaPublishTask.Input input = getInput();
        input.setBootStrapServers("");
        Properties props = manager.getProducerProperties(input);
        Producer producerMock = mock(Producer.class);
        Producer producer = manager.getFromCache(props, () -> producerMock);
        assertNotNull(producer);
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testMaxBlockMsFromConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals(props.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG), "500");
    }

    @Test
    public void testMaxBlockMsFromInput() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        input.setMaxBlockMs(600);
        Properties props = manager.getProducerProperties(input);
        assertEquals(props.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG), "600");
    }

    @Test
    public void testGetProducerWithOverridesAppliesDefaults() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));

        Properties props =
                manager.buildProducerProperties(
                        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092"));

        assertEquals("broker:9092", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("150", props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals("500", props.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
        assertEquals(
                "org.apache.kafka.common.serialization.StringSerializer",
                props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    public void testGetProducerWithOverridesFallsBackToConfiguredBootstrapServers() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        manager.bootStrapServer = "shared-broker:9092";

        Properties props = manager.buildProducerProperties(Map.of());

        assertEquals(
                "shared-broker:9092", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testGetProducerWithOverridesOmitsBootstrapServersWhenUnconfigured() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));

        Properties props = manager.buildProducerProperties(Map.of());

        assertNull(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testGetProducerWithOverridesLayersSharedSslConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        manager.securityProtocol = "SSL";
        manager.sslTrustStoreLocation = "/etc/kafka/truststore.jks";
        manager.sslTrustStorePassword = "trust-pass";
        manager.sslKeystoreLocation = "/etc/kafka/keystore.jks";
        manager.sslKeystorePassword = "key-pass";

        Properties props = manager.buildProducerProperties(Map.of());

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals(
                "/etc/kafka/truststore.jks",
                props.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals(
                "trust-pass", props.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals(
                "/etc/kafka/keystore.jks",
                props.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("key-pass", props.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    }

    @Test
    public void testGetProducerWithOverridesCanOverrideSharedSslConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        manager.securityProtocol = "SSL";

        Properties props =
                manager.buildProducerProperties(
                        Map.of(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"));

        assertEquals(
                "PLAINTEXT", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testGetProducerPropertiesFallsBackToConfiguredBootstrapServersWhenInputBlank() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        manager.bootStrapServer = "shared-broker:9092";
        KafkaPublishTask.Input input = getInput();
        input.setBootStrapServers("");

        Properties props = manager.getProducerProperties(input);

        assertEquals(
                "shared-broker:9092", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testGetProducerPropertiesAppliesSharedSslConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        manager.securityProtocol = "SSL";
        manager.sslKeystoreLocation = "/etc/kafka/keystore.jks";
        KafkaPublishTask.Input input = getInput();

        Properties props = manager.getProducerProperties(input);

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals(
                "/etc/kafka/keystore.jks",
                props.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    }

    private KafkaPublishTask.Input getInput() {
        KafkaPublishTask.Input input = new KafkaPublishTask.Input();
        input.setTopic("testTopic");
        input.setValue("TestMessage");
        input.setKeySerializer(LongSerializer.class.getCanonicalName());
        input.setBootStrapServers("servers");
        return input;
    }
}
