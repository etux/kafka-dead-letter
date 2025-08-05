package org.etux.kafka.deadletter.statestore

data class DeadLetteredMessage<T>(
    val retryCount: Int,
    val uniqueMessageId: String,
    val payload: T, // ByteArray
)