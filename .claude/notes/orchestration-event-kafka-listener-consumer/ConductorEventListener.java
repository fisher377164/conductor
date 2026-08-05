package com.example.kafka.listener;

import com.example.kafka.event.ConductorEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConductorEventListener {

    @KafkaListener(
            topics = "${app.kafka.conductor-event-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "conductorEventKafkaListenerContainerFactory"
    )
    public void consume(ConductorEvent event) {
        log.info(
                "Received Conductor event: eventId={}, eventType={}, executionId={}, status={}",
                event.eventID(),
                event.eventType(),
                event.executionId(),
                event.status()
        );

        try {
            processEvent(event);
        } catch (Exception exception) {
            log.error(
                    "Failed to process Conductor event: eventId={}",
                    event.eventID(),
                    exception
            );

            throw exception;
        }
    }

    private void processEvent(ConductorEvent event) {
        // Add business logic here.
    }
}