package org.etux.kafka.deadletter

enum class DeadLetterCommandType {
    PUT,
    DELETE,
    RETRY,
}