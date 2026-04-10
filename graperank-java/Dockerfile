# -------- Build stage --------
# Pinned 2026-04-06
FROM docker.io/library/maven:3.9.9-eclipse-temurin-21@sha256:4f991cd094285b7e67afeaab05d21bae63bfc81ac94a7c2527dff13851d3188c AS builder

WORKDIR /app

# Copy pom first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests


# -------- Runtime stage --------
# Pinned 2026-04-06
FROM docker.io/library/eclipse-temurin:21-jre@sha256:be2ed5af8104188957a9f8ba1e440cc357f4d0f6f5a5628e84c9b9c5843214e6

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/target/graperank3-1.0-SNAPSHOT.jar app.jar

RUN useradd -r -s /bin/false appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]
