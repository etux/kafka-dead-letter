package org.etux.kafka.deadletter

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ExampleKafkaApplication(
    private val inputTopic: String,
    private val deadLetterTopic: String,
    private val bootstrapServer: String,
    private val store: ReadOnlyKeyValueStore<String, List<String>>,
) {
    enum class MessageType {
        SUCCESSFUL,
        RETRY,
        FAIL_FOREVER,
    }

    private val producerProperties: Properties by lazy {
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    }
    private val producer: KafkaProducer<String, String> = KafkaProducer(producerProperties)

    private val deadLetterProducerProperties: Properties by lazy {
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    }
    private val deadLetterProducer: KafkaProducer<String, String> = KafkaProducer(deadLetterProducerProperties)

    private val consumerProperties: Properties by lazy {
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, "my-application")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
    }
    private val consumer: KafkaConsumer<String, String> = KafkaConsumer(consumerProperties)

    fun run() {
        logger.info("Starting application.")
        Executors
            .newFixedThreadPool(3)
            .also {
                it.submit(::setProducerUp)
                it.submit(::setConsumerUp)
            }
    }

    private fun setProducerUp(): () -> Unit =
        {
            Runtime.getRuntime().addShutdownHook(
                Thread {
                    producer.close()
                },
            )

            logger.info("Starting application producer.")

            var current: Long = 0
            while (true) {
                try {
                    logger.info("Sending random message.")
                    producer
                        .send(
                            ProducerRecord(
                                /* topic = */ inputTopic,
                                /* key = */ "key-${(current++ % 7)}",
                                /* value = */ enumValues<MessageType>().random().name,
                            ),
                        ).get(10_000, TimeUnit.MILLISECONDS)
                    logger.info("Sent random message $current.")
                    sleep(5000)
                } catch (t: Throwable) {
                    logger.warn("Unable to produce record.", t)
                    sleep(10_000)
                }
            }
        }

    private fun setConsumerUp(): () -> Unit {
        Runtime.getRuntime().addShutdownHook(
            Thread {
                consumer.close()
                logger.info("Closed consumer.")
            },
        )

        logger.info("Starting application consumer.")

        consumer.subscribe(listOf(inputTopic))

        while (true) {
            try {
                consumer
                    .poll(Duration.ofMillis(5000))
                    .forEach { record ->
                        if (store.get(record.key()) != null) {
                            throw RetryException(
                                key = record.key(),
                                value = record.value(),
                            )
                        }
                        logger.info("Consuming message key: ${record.key()} value: ${record.value()}.")
                        when (MessageType.valueOf(record.value())) {
                            MessageType.SUCCESSFUL -> logger.info("Message successful.")
                            MessageType.RETRY -> {
                                throw RetryException(
                                    key = record.key(),
                                    value = record.value(),
                                )
                            }
                            MessageType.FAIL_FOREVER -> {
                                logger.info("Failed.")
                                throw RetryException(
                                    key = record.key(),
                                    value = record.value(),
                                )
                            }
                        }
                    }
                sleep(1000)
                logger.info("Finishing consuming records. Re-starting...")
            } catch (retryException: RetryException) {
                logger.warn("Unable to consume.", retryException)
                val producerRecord = ProducerRecord(
                    /* topic = */ deadLetterTopic,
                    /* key = */ retryException.key,
                    /* value = */ retryException.value,
                )
                producerRecord.headers().apply {
                    add("retry-count", 0.toString().toByteArray())
                    add("operation", DeadLetterCommandType.PUT.name.toByteArray())
                }

                deadLetterProducer.send(producerRecord).get(5000, TimeUnit.MILLISECONDS)
                logger.info("Sent error to dead letter topic.", retryException)
                sleep(10_000)
            } catch (t: Throwable) {
                logger.warn("Error while consuming record.", t)
                sleep(60_000)
            }
        }
    }

    class RetryException(
        val key: String,
        val value: String,
        override val cause: Throwable? = null,
    ) : Exception(cause)

    private companion object {
        private val logger = LoggerFactory.getLogger(ExampleKafkaApplication::class.java)
    }
}
