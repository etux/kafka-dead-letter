package org.etux.kafka.deadletter

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore

class DeadLetterProcessorSupplier(private val storeName: String) : ProcessorSupplier<String, String, String, DeadLetterMessage> {
    override fun get(): Processor<String, String, String, DeadLetterMessage> {
        return DeadLetterProcessor(storeName)
    }
}

class DeadLetterProcessor(private val storeName: String): Processor<String, String, String, DeadLetterMessage> {

    private lateinit var processorContext: ProcessorContext<String, DeadLetterMessage>
    private lateinit var keyValueStore: KeyValueStore<String, DeadLetterMessage>

    override fun init(context: ProcessorContext<String, DeadLetterMessage>?) {
        this.processorContext = context ?: throw IllegalArgumentException("ProcessorContext cannot be null")
        this.keyValueStore = context.getStateStore(storeName) as KeyValueStore<String, DeadLetterMessage>
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

        val existingValues = keyValueStore.get(key)

        val newValue = when (operation) {
            DeadLetterCommandType.PUT -> {
                when (existingValues) {
                    null -> DeadLetterMessage(
                        retryCount = retryCount ?: 0,
                        messages = listOf(value),
                    )
                    else -> DeadLetterMessage(
                        retryCount = existingValues.retryCount + (retryCount ?: 0),
                        messages = existingValues.messages + value,
                    )

                }
            }

            DeadLetterCommandType.DELETE -> {
                when (existingValues) {
                    null -> throw IllegalStateException("Key $key does not exist in the store.")
                    else -> DeadLetterMessage(
                        retryCount = 0,
                        messages = existingValues.messages.filterNot { it == value })

                }
            }

            DeadLetterCommandType.RETRY -> {
                when (existingValues) {
                    null -> throw IllegalStateException("Key $key does not exist in the store.")
                    else -> DeadLetterMessage(

                            retryCount = existingValues.retryCount + 1,
                            messages = existingValues.messages
                        )
                }
            }
        }

        keyValueStore.put(/* key = */ record.key(), /* value = */ newValue)
    }
}

data class DeadLetterMessage(
    val retryCount: Int,
    val messages: List<String>,
)