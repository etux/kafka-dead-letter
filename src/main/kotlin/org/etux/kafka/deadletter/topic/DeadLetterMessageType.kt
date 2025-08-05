package org.etux.kafka.deadletter.topic

enum class DeadLetterMessageType {
    PUT,
    DELETE,
    RETRY,
}