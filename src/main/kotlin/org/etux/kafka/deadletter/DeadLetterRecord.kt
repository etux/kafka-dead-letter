package org.etux.kafka.deadletter

import java.time.Instant

data class DeadLetterRecord<K, V> (
    val retries: Int = 0,
    val lastRetry: Instant? = null,
    val isPaused: Boolean = false,
    val key: K,
    val value: V,
)