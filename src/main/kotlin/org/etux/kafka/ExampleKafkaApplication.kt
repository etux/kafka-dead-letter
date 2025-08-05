package org.etux.kafka

import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors

class ExampleKafkaApplication(
    private val inputTopic: String,
    private val consumer: ExampleConsumerThatIsAbleToDeadLetter<String, String>, // TODO: generic consumer
) {
    private val logger = LoggerFactory.getLogger(ExampleKafkaApplication::class.java)

    fun run() {
        logger.info("Starting application.")
        Executors
            .newFixedThreadPool(3)
            .submit(::setConsumerUp)
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
            consumer.poll(Duration.ofMillis(5000))
            logger.info("Polled consumer for messages.")
        }
    }
}
