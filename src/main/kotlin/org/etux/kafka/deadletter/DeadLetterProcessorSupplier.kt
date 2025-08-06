package org.etux.kafka.deadletter

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorSupplier

class DeadLetterProcessorSupplier<K, V>(
    private val deadLetterStoreName: String,
    private val reprocessIntervalInSeconds: Long,
    private val processingMode: DeadLetterProcessor.Mode,
    private val businessLogic: (key: K, value: V) -> Unit,
) :
    ProcessorSupplier<K, V, K, List<DeadLetteredValue<V>>> {
    override fun get(): Processor<K, V, K, List<DeadLetteredValue<V>>> {
        return DeadLetterProcessor(
            deadLetterStoreName = deadLetterStoreName,
            reprocessIntervalInSeconds = reprocessIntervalInSeconds,
            maxRetries = 3,
            processingMode = processingMode,
            businessLogic = businessLogic
        )
    }
}