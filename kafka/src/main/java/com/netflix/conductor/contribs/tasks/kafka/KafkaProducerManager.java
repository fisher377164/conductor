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
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import jakarta.annotation.PostConstruct;

@SuppressWarnings("rawtypes")
@Component
public class KafkaProducerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerManager.class);

    /**
     * Cluster-wide bootstrap servers/SSL config. Shared by every producer this manager builds,
     * whether created from a {@link KafkaPublishTask.Input} or from {@link #getProducerForOverrides}.
     * Package-private (rather than {@code private}) so tests in this package can set them directly
     * without a Spring context.
     */
    @Value("${bootstrap.servers:}")
    String bootStrapServer;

    @Value("${properties.security.protocol:}")
    String securityProtocol;

    @Value("${ssl.truststore.location:}")
    String sslTrustStoreLocation;

    @Value("${ssl.truststore.password:}")
    String sslTrustStorePassword;

    @Value("${ssl.keystore.location:}")
    String sslKeystoreLocation;

    @Value("${ssl.keystore.password:}")
    String sslKeystorePassword;

    private final String requestTimeoutConfig;
    private final Cache<Properties, Producer> kafkaProducerCache;
    private final String maxBlockMsConfig;

    private static final String STRING_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    private static final RemovalListener<Properties, Producer> LISTENER =
            notification -> {
                if (notification.getValue() != null) {
                    notification.getValue().close();
                    LOGGER.debug("Closed producer for {}", notification.getKey());
                }
            };

    public KafkaProducerManager(
            @Value("${conductor.tasks.kafka-publish.requestTimeout:100ms}") Duration requestTimeout,
            @Value("${conductor.tasks.kafka-publish.maxBlock:500ms}") Duration maxBlock,
            @Value("${conductor.tasks.kafka-publish.cacheSize:10}") int cacheSize,
            @Value("${conductor.tasks.kafka-publish.cacheTime:120000ms}") Duration cacheTime) {
        this.requestTimeoutConfig = String.valueOf(requestTimeout.toMillis());
        this.maxBlockMsConfig = String.valueOf(maxBlock.toMillis());
        this.kafkaProducerCache =
                CacheBuilder.newBuilder()
                        .removalListener(LISTENER)
                        .maximumSize(cacheSize)
                        .expireAfterAccess(cacheTime.toMillis(), TimeUnit.MILLISECONDS)
                        .build();
    }

    @PostConstruct
    public void onPostConstruct() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(" == config properties ==");
            System.getProperties()
                    .forEach(
                            (k, v) -> {
                                String key = k.toString();
                                if (key.startsWith("ssl.")
                                        || key.startsWith("sasl.")
                                        || key.equals("bootstrap.servers")
                                        || key.contains("security.protocol")
                                        || key.startsWith("conductor.tasks.kafka-")) {
                                    LOGGER.debug(
                                            "  {} = {}",
                                            key,
                                            key.toLowerCase().matches(".*(password|secret|token).*")
                                                    ? "********"
                                                    : v);
                                }
                            });
        }
    }

    public Producer getProducer(KafkaPublishTask.Input input) {
        Properties configProperties = getProducerProperties(input);
        return getFromCache(configProperties, () -> new KafkaProducer(configProperties));
    }

    /**
     * Returns a cached producer for callers outside the kafka-publish task, e.g. workflow status
     * listeners, that supply their own Kafka producer properties (bootstrap servers, SSL/SASL,
     * etc.) instead of a {@link KafkaPublishTask.Input}. Properties are layered the same way as the
     * task path: this manager's own shared defaults first, then the caller-supplied overrides on
     * top, so a caller can override any {@link ProducerConfig} key while inheriting everything else
     * (the cluster's broker address, SSL/SASL, timeouts) unchanged.
     */
    @SuppressWarnings("unchecked")
    public Producer<String, String> getProducerForOverrides(Map<String, Object> producerOverrides) {
        Properties configProperties = buildProducerProperties(producerOverrides);
        return (Producer<String, String>)
                getFromCache(configProperties, () -> new KafkaProducer<>(configProperties));
    }

    @VisibleForTesting
    Properties buildProducerProperties(Map<String, Object> producerOverrides) {
        Properties configProperties = new Properties();
        applySharedDefaults(configProperties);
        if (producerOverrides != null) {
            configProperties.putAll(producerOverrides);
        }
        return configProperties;
    }

    @VisibleForTesting
    Producer getFromCache(Properties configProperties, Callable<Producer> createProducerCallable) {
        try {
            return kafkaProducerCache.get(configProperties, createProducerCallable);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Properties getProducerProperties(KafkaPublishTask.Input input) {
        Properties configProperties = new Properties();
        applySharedDefaults(configProperties);

        applyConfig(
                configProperties,
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                input.getBootStrapServers());
        applyConfig(
                configProperties, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, input.getKeySerializer());
        applyConfig(
                configProperties,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                input.getRequestTimeoutMs());
        applyConfig(configProperties, ProducerConfig.MAX_BLOCK_MS_CONFIG, input.getMaxBlockMs());
        return configProperties;
    }

    /**
     * Base layer shared by every producer this manager builds: this manager's own
     * request-timeout/max-block/serializer defaults, plus the cluster-wide bootstrap
     * servers/SSL config. Callers layer their own overrides on top of this by applying
     * {@link #applyConfig} (or {@code Properties.putAll}) afterwards.
     */
    private void applySharedDefaults(Properties configProperties) {
        applyConfig(
                configProperties, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, null, bootStrapServer);
        applyConfig(
                configProperties,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                null,
                STRING_SERIALIZER);
        applyConfig(
                configProperties,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                null,
                STRING_SERIALIZER);
        applyConfig(
                configProperties,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                null,
                requestTimeoutConfig);
        applyConfig(configProperties, ProducerConfig.MAX_BLOCK_MS_CONFIG, null, maxBlockMsConfig);
        applyConfig(
                configProperties,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                null,
                securityProtocol);
        applyConfig(
                configProperties,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                null,
                sslTrustStoreLocation);
        applyConfig(
                configProperties,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                null,
                sslTrustStorePassword);
        applyConfig(
                configProperties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, null, sslKeystoreLocation);
        applyConfig(
                configProperties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, null, sslKeystorePassword);
    }

    private static void applyConfig(Properties config, final String key, Object inputValue) {
        applyConfig(config, key, inputValue, null);
    }

    /** resolution: caller-supplied value -> System property -> Spring {@code @Value} default. */
    private static void applyConfig(
            Properties config, final String key, Object inputValue, Object defaultValue) {
        String value = Objects.toString(inputValue, null);
        if (StringUtils.isEmpty(value)) {
            value = System.getProperty(key);
            if (StringUtils.isEmpty(value)) {
                value = Objects.toString(defaultValue, null);
            }
        }
        if (StringUtils.isNotEmpty(value)) {
            config.put(key, value);
        }
    }
}
