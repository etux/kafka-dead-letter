package org.etux.kafka.deadletter

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore

class DeadLetterProcessorSupplier(private val storeName: String) : ProcessorSupplier<String, String, String, List<DeadLetterMessage>> {
    override fun get(): Processor<String, String, String, List<DeadLetterMessage>> {
        return DeadLetterProcessor(storeName)
    }
}

class DeadLetterProcessor(private val storeName: String): Processor<String, String, String, List<DeadLetterMessage>> {

    private lateinit var processorContext: ProcessorContext<String, List<DeadLetterMessage>>
    private lateinit var keyValueStore: KeyValueStore<String, List<DeadLetterMessage>>

    override fun init(context: ProcessorContext<String, List<DeadLetterMessage>>?) {
        this.processorContext = context ?: throw IllegalArgumentException("ProcessorContext cannot be null")
        this.keyValueStore = context.getStateStore(storeName) as KeyValueStore<String, List<DeadLetterMessage>>
    }

    override fun process(record: Record<String, String>) {
        val key = record.key()
        val value = record.value()
        val headers = record.headers()

        val operation = headers.lastHeader("operation")
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?.let { DeadLetterCommandType.valueOf(it) }
            ?: throw IllegalArgumentException("Header 'operation' is missing")

        val retryCount = headers.lastHeader("retry-count")
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?.toIntOrNull()

        val uniqueMessageId = headers.lastHeader("unique-message-id")
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?: throw IllegalArgumentException("Header 'unique-message-id' is missing")

        val deadLetteredMessagesForKey = keyValueStore.get(key)

        val newValue = when (operation) {
            DeadLetterCommandType.PUT -> {
                when (deadLetteredMessagesForKey) {
                    null -> listOf(DeadLetterMessage(
                        retryCount = 0,
                        uniqueMessageId = uniqueMessageId,
                        message = value,
                    ))
                    else -> deadLetteredMessagesForKey + DeadLetterMessage(
                        retryCount = 0,
                        uniqueMessageId = uniqueMessageId,
                        message = value,
                    )

                }
            }

            DeadLetterCommandType.DELETE -> {
                when (deadLetteredMessagesForKey) {
                    null -> throw IllegalStateException("Key $key does not exist in the store.")
                    else -> deadLetteredMessagesForKey.filterNot { it.uniqueMessageId == uniqueMessageId }

                }
            }

            DeadLetterCommandType.RETRY -> {
                when (deadLetteredMessagesForKey) {
                    null -> throw IllegalStateException("Key $key does not exist in the store.")
                    else -> deadLetteredMessagesForKey.map {
                        if (it.uniqueMessageId == uniqueMessageId) {
                            it.copy(retryCount = (it.retryCount + 1))
                        } else {
                            it
                        }
                    }
                }
            }
        }

        keyValueStore.put(/* key = */ record.key(), /* value = */ newValue)
    }
}

data class DeadLetterMessage(
    val retryCount: Int,
    val uniqueMessageId: String,
    val message: String, // ByteArray
)