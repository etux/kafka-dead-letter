package org.etux.kafka.deadletter

import java.util.UUID

data class DeadLetteredValue<V>(
    val retryCount: Int,
    val uniqueMessageId: UUID,
    val payload: V,
)