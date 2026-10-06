package com.digiteen.consumer;

import java.time.Instant;
import java.util.UUID;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.DataAccessResourceFailureException;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventListenerTest {
    private final ObjectMapper mapper = new RabbitTopology().eventObjectMapper();
    private final ProcessedEventService service = mock(ProcessedEventService.class);
    private final EventListener listener = new EventListener(mapper, service);

    static WalletOperationEvent event(String type, Long source, Long destination) {
        return new WalletOperationEvent(UUID.randomUUID(), 1, 42, type, 100, source,
                destination, Instant.parse("2026-01-01T12:00:00Z"), UUID.randomUUID());
    }

    private Message message(WalletOperationEvent event) {
        return new Message(mapper.writeValueAsBytes(event), new MessageProperties());
    }

    @Test
    void validTypesRoundTripAndRejectInvalidSides() {
        for (var event : new WalletOperationEvent[]{event("DEPOSIT", null, 1L),
                event("WITHDRAWAL", 1L, null), event("TRANSFER", 1L, 2L)}) {
            var decoded = mapper.readValue(mapper.writeValueAsBytes(event), WalletOperationEvent.class);
            decoded.validate();
            assertThat(decoded).isEqualTo(event);
        }
        for (var event : new WalletOperationEvent[]{event("OTHER", null, 1L),
                event("DEPOSIT", 1L, 2L), event("DEPOSIT", null, null),
                event("WITHDRAWAL", null, 1L), event("TRANSFER", 1L, 1L),
                event("TRANSFER", -1L, 2L), event("TRANSFER", 1L, null)}) {
            assertThatThrownBy(() -> listener.consume(message(event)))
                    .isInstanceOf(AmqpRejectAndDontRequeueException.class).hasNoCause();
        }
        verifyNoInteractions(service);
    }

    @Test
    void malformedUnsupportedAndNonIntegralEventsNeverReachDatabase() {
        var event = event("DEPOSIT", null, 1L);
        String json = mapper.writeValueAsString(event);
        for (String invalid : new String[]{"{", "null", json + " {}",
                json.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                json.replace("\"amount\":100", "\"amount\":0"),
                json.replace("\"amount\":100", "\"amount\":100.5"),
                json.replace("\"amount\":100", "\"amount\":\"100\""),
                json.replace("\"amount\":100", "\"amount\":null")}) {
            assertThatThrownBy(() -> listener.consume(new Message(invalid.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    new MessageProperties()))).isInstanceOf(AmqpRejectAndDontRequeueException.class).hasNoCause();
        }
        verifyNoInteractions(service);
    }

    @Test
    void commitLogFollowsServiceReturnAndMdcIsRestoredEvenOnFailure() {
        Logger logger = (Logger) LoggerFactory.getLogger(EventListener.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        var event = event("DEPOSIT", null, 1L);
        try {
            MDC.put("traceId", "existing-context");
            when(service.record(event)).thenAnswer(invocation -> {
                assertThat(MDC.get("traceId")).isEqualTo(event.traceId().toString());
                assertThat(appender.list).isEmpty();
                return true;
            });
            listener.consume(message(event));
            assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                    .containsExactly("consumption_committed");
            assertThat(MDC.get("traceId")).isEqualTo("existing-context");
            doReturn(false).when(service).record(event);
            listener.consume(message(event));
            assertThat(appender.list.getLast().getFormattedMessage()).isEqualTo("consumption_duplicate");
            int beforeFailure = appender.list.size();
            when(service.record(event)).thenThrow(new DataAccessResourceFailureException("unavailable"));
            assertThatThrownBy(() -> listener.consume(message(event)))
                    .isInstanceOf(DataAccessResourceFailureException.class);
            assertThat(appender.list).hasSize(beforeFailure + 1);
            ILoggingEvent failed = appender.list.getLast();
            assertThat(failed.getFormattedMessage()).isEqualTo("consumption_failed");
            assertThat(failed.getMDCPropertyMap()).containsEntry("traceId", event.traceId().toString());
            assertThat(failed.getKeyValuePairs()).anySatisfy(pair -> {
                assertThat(pair.key).isEqualTo("eventId");
                assertThat(pair.value).isEqualTo(event.eventId());
            });
            assertThat(failed.getThrowableProxy()).isNull();
            assertThat(MDC.get("traceId")).isEqualTo("existing-context");
            MDC.remove("traceId");
            assertThatThrownBy(() -> listener.consume(message(event)))
                    .isInstanceOf(DataAccessResourceFailureException.class);
            assertThat(MDC.get("traceId")).isNull();
        } finally {
            MDC.remove("traceId");
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
