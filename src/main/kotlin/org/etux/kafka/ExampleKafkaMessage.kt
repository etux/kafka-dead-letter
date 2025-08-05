package org.etux.kafka

data class ExampleKafkaMessage(val type: MessageType) {
    enum class MessageType {
        SUCCESSFUL,
        RETRY,
        FAIL_FOREVER,
    }
}