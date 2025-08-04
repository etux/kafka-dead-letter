FROM openjdk:17-jdk-slim
ENV KAFKA_VERSION=3.7.2 \
    SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y curl && \
    curl -s https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
ENV PATH="/opt/kafka/bin:$PATH"
ENV JAVA_TOOL_OPTIONS="-XX:+IgnoreUnrecognizedVMOptions -XX:-UseContainerSupport -Djdk.internal.platform.disableContainerSupport=true"