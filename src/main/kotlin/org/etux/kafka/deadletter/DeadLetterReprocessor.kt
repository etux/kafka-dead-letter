package org.etux.kafka.deadletter

import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.etux.kafka.ProcessingMode
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage
import org.etux.kafka.deadletter.topic.DeadLetterMessageType
import org.etux.kafka.deadletter.topic.DeadLetterProducer
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.concurrent.Executors

class DeadLetterReprocessor<K, V>(
    private val processingMode: ProcessingMode,
    private val deadLetterStateStore: ReadOnlyKeyValueStore<K, List<DeadLetteredMessage<V>>>,
    private val deadLetterProducer: DeadLetterProducer<K, V>,
    private val periodInSeconds: Long,
    private val businessLogic: (V) -> Boolean,
) {
    private val logger = LoggerFactory.getLogger(DeadLetterReprocessor::class.java)

    fun run() {
        logger.info("Reprocessing dead-lettered messages.")

        Executors
            .newSingleThreadExecutor()
            .submit {
                while (true) {
                    try {
                        logger.debug("Polling dead-letter state store")
                        deadLetterStateStore.all().forEach { keyValue ->
                            logger.info("Found $keyValue")

                            when (processingMode) {
                                ProcessingMode.DELTA -> {
                                    keyValue.value.forEach messagesWithSameKey@{ message ->
                                        val successfullyProcessed = businessLogic(message.payload)

                                        if (successfullyProcessed) {
                                            deadLetterProducer.publishDeadLetter(
                                                key = keyValue.key,
                                                value = message.payload,
                                                operation = DeadLetterMessageType.DELETE,
                                                uniqueMessageId = message.uniqueMessageId,
                                            )
                                        } else {
                                            deadLetterProducer.publishDeadLetter(
                                                key = keyValue.key,
                                                value = message.payload,
                                                operation = DeadLetterMessageType.RETRY,
                                                uniqueMessageId = message.uniqueMessageId,
                                            )
                                            return@messagesWithSameKey
                                        }
                                        logger.info("Message with key: ${keyValue.key} and value: ${message.payload} failed processing, sending to dead-letter topic.")
                                    }
                                }
                                ProcessingMode.ABSOLUTE -> {
                                    keyValue.value.last().let { message ->
                                        val successfullyProcessed = businessLogic(message.payload)

                                        if (successfullyProcessed) {
                                            keyValue.value.forEach { deadLetteredMessage ->
                                                deadLetterProducer.publishDeadLetter(
                                                    key = keyValue.key,
                                                    value = deadLetteredMessage.payload,
                                                    operation = DeadLetterMessageType.DELETE,
                                                    uniqueMessageId = deadLetteredMessage.uniqueMessageId,
                                                )
                                            }
                                        } else {
                                            deadLetterProducer.publishDeadLetter(
                                                key = keyValue.key,
                                                value = message.payload,
                                                operation = DeadLetterMessageType.RETRY,
                                                uniqueMessageId = message.uniqueMessageId,
                                            )
                                        }
                                        logger.info("Message with key: ${keyValue.key} and value: ${message.payload} will be retried, retry count: ${message.retryCount + 1}.")
                                    }
                                }
                            }
                        }
                        sleep(periodInSeconds * 1000)
                    } catch (e: InvalidStateStoreException) {
                        logger.warn("Unable to work with the state store.", e)
                        sleep(60_000)
                    } catch (t: Throwable) {
                        logger.warn("Error while reading the dead letter state store.", t)
                        sleep(120_000)
                    }
                }
            }
    }
}