package org.etux.kafka.deadletter

import java.io.Serializable
import java.util.UUID

data class DeadLetteredValue<V>(
    val retryCount: Int,
    val uniqueMessageId: UUID,
    val payload: V,
    val cause: String? = null,
    val stackTrace: String? = null,
    val headers: Map<String, String> = emptyMap(),
): Serializable