package org.etux.kafka.deadletter

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage
import org.etux.kafka.deadletter.topic.DeadLetterMessageType

class DeadLetterProcessor(private val storeName: String): Processor<String, String, String, List<DeadLetteredMessage<String>>> {

    private lateinit var processorContext: ProcessorContext<String, List<DeadLetteredMessage<String>>>
    private lateinit var keyValueStore: KeyValueStore<String, List<DeadLetteredMessage<String>>>

    override fun init(context: ProcessorContext<String, List<DeadLetteredMessage<String>>>?) {
        this.processorContext = context ?: throw IllegalArgumentException("ProcessorContext cannot be null")
        this.keyValueStore = context.getStateStore(storeName) as KeyValueStore<String, List<DeadLetteredMessage<String>>>
    }

    override fun process(record: Record<String, String>) {
        val key = record.key()
        val value = record.value()
        val headers = record.headers()

        val operation = headers.lastHeader("operation")
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?.let { DeadLetterMessageType.valueOf(it) }
            ?: throw IllegalArgumentException("Header 'operation' is missing")

        val uniqueMessageId = headers.lastHeader("unique-message-id")
            ?.value()
            ?.toString(Charsets.UTF_8)
            ?: throw IllegalArgumentException("Header 'unique-message-id' is missing")

        val deadLetteredMessagesForKey = keyValueStore.get(key)

        when (operation) {
            DeadLetterMessageType.PUT -> {
                when (deadLetteredMessagesForKey) {
                    null -> listOf(
                        DeadLetteredMessage(
                            retryCount = 0,
                            uniqueMessageId = uniqueMessageId,
                            payload = value,
                        )
                    )
                    else -> deadLetteredMessagesForKey + DeadLetteredMessage(
                        retryCount = 0,
                        uniqueMessageId = uniqueMessageId,
                        payload = value,
                    )
                }.also {
                    keyValueStore.put(
                        /* key = */ key,
                        /* value = */ it,
                    )
                }
            }

            DeadLetterMessageType.DELETE -> {
                when (deadLetteredMessagesForKey) {
                    null -> throw IllegalStateException("Key $key does not exist in the store. Requested operation: DELETE")
                    else -> deadLetteredMessagesForKey
                        .filterNot { it.uniqueMessageId == uniqueMessageId }
                        .also {
                            if (it.isEmpty()) {
                                keyValueStore.delete(key)
                            } else {
                                keyValueStore.put(
                                    /* key = */ key,
                                    /* value = */ it,
                                )
                            }
                        }
                }
            }

            DeadLetterMessageType.RETRY -> {
                when (deadLetteredMessagesForKey) {
                    null -> throw IllegalStateException("Key $key does not exist in the store. Requested operation: RETRY")
                    else -> {
                        deadLetteredMessagesForKey.map {
                            if (it.uniqueMessageId == uniqueMessageId) {
                                it.copy(retryCount = (it.retryCount + 1))
                            } else {
                                it
                            }
                        }.also {
                            keyValueStore.put(
                                /* key = */ key,
                                /* value = */ it,
                            )
                        }
                    }
                }

            }
        }

        processorContext.commit()
    }
}
