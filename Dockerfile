FROM maven:3.9.12-eclipse-temurin-25@sha256:4f82a03a7d6679281952d628131299b1be88d7030a49c6a2b7d2ba2642e44e3e AS build
WORKDIR /app
COPY pom.xml .
COPY .mvn .mvn
RUN mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp dependency:go-offline
COPY src src
RUN mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp package -DskipTests

FROM eclipse-temurin:25.0.2_10-jre@sha256:1089975c9ab22e822faf568c0e03997ee707165a125b7a55fbc799315b63d697
WORKDIR /app
COPY --from=build /app/target/wallet-event-consumer-0.1.0.jar app.jar
COPY --from=build /app/target/classes/com/digiteen/consumer/HealthCheck.class /app/health/com/digiteen/consumer/HealthCheck.class
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 CMD ["java", "-cp", "/app/health", "com.digiteen.consumer.HealthCheck"]
ENTRYPOINT ["java", "-jar", "app.jar"]
