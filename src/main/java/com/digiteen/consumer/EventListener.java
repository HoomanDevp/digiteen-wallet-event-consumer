package com.digiteen.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class EventListener {
    private static final Logger log = LoggerFactory.getLogger(EventListener.class);
    private final ObjectMapper eventObjectMapper;
    private final ProcessedEventService service;

    public EventListener(ObjectMapper eventObjectMapper, ProcessedEventService service) {
        this.eventObjectMapper = eventObjectMapper;
        this.service = service;
    }

    @RabbitListener(id = "walletAudit", queues = RabbitTopology.QUEUE)
    public void consume(Message message) {
        WalletOperationEvent event;
        try {
            event = eventObjectMapper.readValue(message.getBody(), WalletOperationEvent.class);
            if (event == null) {
                throw new IllegalArgumentException("Missing event");
            }
            event.validate();
        } catch (JacksonException | IllegalArgumentException invalid) {
            // Never retain the parsing cause: it can contain the original payload.
            throw new AmqpRejectAndDontRequeueException("Invalid or unsupported wallet event");
        }
        String previousTraceId = MDC.get("traceId");
        try (var ignored = MDC.putCloseable("traceId", event.traceId().toString())) {
            // The separate service proxy commits before returning. AUTO ack follows this listener.
            boolean inserted = service.record(event);
            log.atInfo().addKeyValue("eventId", event.eventId())
                    .addKeyValue("transactionId", event.transactionId())
                    .addKeyValue("traceId", event.traceId()).addKeyValue("type", event.transactionType())
                    .log(inserted ? "consumption_committed" : "consumption_duplicate");
        } finally {
            if (previousTraceId == null) {
                MDC.remove("traceId");
            } else {
                MDC.put("traceId", previousTraceId);
            }
        }
    }
}
