package org.etux.kafka.deadletter

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.Properties
import java.util.concurrent.Executors

class DeadLetterPlayer(
    inputStore: String,
    properties: Properties,
    topology: Topology,
) {

    private val streams = KafkaStreams(
        /* topology = */ topology,
        /* props = */ properties,
    )

    val store: ReadOnlyKeyValueStore<String, List<DeadLetterMessage>>

    init {
        streams.start()
        store = streams.store(
            StoreQueryParameters.fromNameAndType(
                /* storeName = */ inputStore,
                /* queryableStoreType = */ QueryableStoreTypes.keyValueStore()
            )
        )
    }

    fun run() {
        logger.info("Running Dead Letter Queryer.")

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("Shutting down Kafka Streams.")
            streams.close()
        })

        logger.info("starting to poll the dead letter table.")

        while(streams.state() != KafkaStreams.State.RUNNING) {
            logger.debug("Waiting for streams to be running.")
            sleep(500)
        }

        Executors
            .newSingleThreadExecutor()
            .submit {
                while (true) {
                    try {
                        logger.info("Polling dead letter table...")
                        store
                            .all()
                            .forEach { keyValue ->
                                // TODO add lambda to reference the business logic
                                logger.info("Found $keyValue")
                            }
                        logger.info("Ran the state store queryer.")
                        sleep(5000)
                    } catch(e: InvalidStateStoreException) {
                        logger.warn("Unable to work with the state store.", e)
                        sleep(60_000)
                    }catch(t: Throwable) {
                        logger.warn("Error while reading the dead letter state store.", t)
                        sleep(120_000)
                    }
                }
            }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DeadLetterPlayer::class.java)
    }
}