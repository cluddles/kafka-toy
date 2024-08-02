package com.cluddles.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

class SimpleConsumer(server: String, consumerGroupId: String, topic: String) {

    constructor(host: String, port: Int, consumerGroupId: String, topic: String):
            this("$host:$port", consumerGroupId, topic)

    private val consumer = KafkaConsumer<String, String> (
        mapOf(
            // Where the kafka server is running
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            // Consumer group
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            // How far back we want to consume messages from
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET,
            // The message key type
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            // The type of the actual message
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        )
    ).apply { this.subscribe(listOf(topic)) }

    init {
        logger.info { "SimpleConsumer started, server: $server, consumerGroupId: $consumerGroupId, topic: $topic" }
    }

    fun consumeForever() {
        while (true) {
            consumeOnce(Duration.ofMillis(100L))
        }
    }

    private fun consumeOnce(pollDuration: Duration) {
        consumer.poll(pollDuration).forEach {
            logger.info { "RECV: ${it.value()}" }
        }
    }

    companion object {
        private const val OFFSET_RESET = "earliest"

        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val consumer = SimpleConsumer("localhost", 29092, "transactions-consumer", "transactions")
    consumer.consumeForever()
}
