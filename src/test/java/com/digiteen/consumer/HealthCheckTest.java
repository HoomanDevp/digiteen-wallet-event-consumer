package com.digiteen.consumer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HealthCheckTest {
    @Test
    void checksHttpStatusAndFailsClosedWhenServerIsUnavailable() throws Exception {
        var status = new AtomicInteger(200);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/actuator/health", exchange -> {
            exchange.sendResponseHeaders(status.get(), -1);
            exchange.close();
        });
        server.start();
        var endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/actuator/health");
        try {
            assertThat(HealthCheck.check(endpoint)).isTrue();
            status.set(503);
            assertThat(HealthCheck.check(endpoint)).isFalse();
        } finally {
            server.stop(0);
        }
        assertThat(HealthCheck.check(endpoint)).isFalse();
    }
}
