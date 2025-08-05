package org.etux.kafka.deadletter

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage

class DeadLetterProcessorSupplier(private val storeName: String) :
    ProcessorSupplier<String, String, String, List<DeadLetteredMessage<String>>> {
    override fun get(): Processor<String, String, String, List<DeadLetteredMessage<String>>> {
        return DeadLetterProcessor(storeName)
    }
}