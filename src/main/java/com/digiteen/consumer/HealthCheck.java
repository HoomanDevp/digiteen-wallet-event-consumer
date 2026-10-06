package com.digiteen.consumer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** JRE-only Docker probe: no shell, curl, or Spring dependencies. */
public final class HealthCheck {
    private HealthCheck() { }

    public static void main(String[] args) {
        System.exit(check(URI.create("http://127.0.0.1:8080/actuator/health")) ? 0 : 1);
    }

    static boolean check(URI endpoint) {
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()) {
            var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(3)).GET().build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        } catch (IOException unavailable) {
            return false;
        }
    }
}
