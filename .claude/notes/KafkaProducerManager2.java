package com.netflix.conductor.contribs.tasks.kafka;

@SuppressWarnings("rawtypes")
@Component
public class KafkaProducerManager2 {

        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerManager.class);

        @Value("${bootstrap.servers:}")
        private String bootStrapServer;

        @Value("${properties.security.protocol:}")
        private String securityProtocol;

        @Value("${ssl.truststore.location:}")
        private String sslTrustStoreLocation;

        @Value("${ssl.truststore.password:}")
        private String sslTrustStorePassword;

        @Value("${ssl.keystore.location:}")
        private String sslKeystoreLocation;

        @Value("${ssl.keystore.password:}")
        private String sslKeystorePassword;

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

            LOGGER.info("bootstrap.servers , bootstrap.servers {}", bootStrapServer);
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
            LOGGER.info("Kafka properties defaultValue , input {}", input);
            Properties configProperties = new Properties();

            applyConfig(
                    configProperties,
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    input.getBootStrapServers(),
                    bootStrapServer);

            applyConfig(
                    configProperties,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    input.getKeySerializer(),
                    STRING_SERIALIZER);

            applyConfig(
                    configProperties,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    null,
                    STRING_SERIALIZER);

            applyConfig(
                    configProperties,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    input.getRequestTimeoutMs(),
                    requestTimeoutConfig);

            applyConfig(
                    configProperties,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG,
                    input.getMaxBlockMs(),
                    maxBlockMsConfig);

            if (!Boolean.FALSE.equals(input.getUseSSL())) {
                applyConfig(
                        configProperties,
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        null,
                        securityProtocol);
                applyConfig(
                        configProperties,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                        input.getSslTruststoreLocation(),
                        sslTrustStoreLocation);
                applyConfig(
                        configProperties,
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                        input.getSslTruststorePassword(),
                        sslTrustStorePassword);
                applyConfig(
                        configProperties,
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        input.getSslKeystoreLocation(),
                        sslKeystoreLocation);
                applyConfig(
                        configProperties,
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        input.getSslKeystorePassword(),
                        sslKeystorePassword);
                applyConfig(
                        configProperties,
                        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                        input.getSslEndpointIdentificationAlgorithm());
            }

            return configProperties;
        }

        private static void applyConfig(Properties config, final String key, Object inputValue) {
            applyConfig(config, key, inputValue, null);
        }

        /** resolution: workflow input -> System property -> Spring @Value default. */
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

        private String getConfigValue(String defaultValue, String valueFromProps) {
            LOGGER.info(
                    "Kafka properties defaultValue , valueFromProps {} {}",
                    defaultValue,
                    valueFromProps);
            return StringUtils.isNotEmpty(defaultValue) ? defaultValue : valueFromProps;
        }
    }