package com.digiteen.consumer;

import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class RabbitTopology {
    public static final String EXCHANGE = "wallet.events";
    public static final String QUEUE = "wallet.events.audit";
    public static final String ROUTING_KEY = "wallet.operation.succeeded";

    @Bean
    DirectExchange walletEvents() { return new DirectExchange(EXCHANGE, true, false); }

    @Bean
    Queue auditQueue() { return new Queue(QUEUE, true, false, false); }

    @Bean
    Binding auditBinding(Queue auditQueue, DirectExchange walletEvents) {
        return BindingBuilder.bind(auditQueue).to(walletEvents).with(ROUTING_KEY);
    }

    @Bean
    ObjectMapper eventObjectMapper() {
        return JsonMapper.builder().disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        var factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        factory.setDefaultRequeueRejected(true);
        factory.setPrefetchCount(10);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        // The default error handler dumps the failed Message (including body); log no payload/cause.
        factory.setErrorHandler(error -> LoggerFactory.getLogger(EventListener.class).atWarn()
                .addKeyValue("errorType", error.getClass().getSimpleName()).log("consumption_failed"));
        return factory;
    }
}
