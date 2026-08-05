package com.example.kafka.config;

import com.example.kafka.event.ConductorEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    @Bean
    public ConsumerFactory<String, ConductorEvent> conductorEventConsumerFactory(
            KafkaProperties kafkaProperties
    ) {
        Map<String, Object> properties =
                new HashMap<>(kafkaProperties.buildConsumerProperties());

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class
        );

        JsonDeserializer<ConductorEvent> valueDeserializer =
                new JsonDeserializer<>(ConductorEvent.class);

        valueDeserializer.addTrustedPackages(
                ConductorEvent.class.getPackageName()
        );

        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                valueDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ConductorEvent>
    conductorEventKafkaListenerContainerFactory(
            ConsumerFactory<String, ConductorEvent> conductorEventConsumerFactory
    ) {
        var factory =
                new ConcurrentKafkaListenerContainerFactory<String, ConductorEvent>();

        factory.setConsumerFactory(conductorEventConsumerFactory);
        factory.setConcurrency(1);

        return factory;
    }
}