package com.cluddles.kafka.avro

import avro.Fruit
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

class AvroConsumer(server: String, consumerGroupId: String, topic: String, registryUrl: String) {

    constructor(host: String, port: Int, consumerGroupId: String, topic: String):
            this("$host:$port", consumerGroupId, topic, AvroConstants.REGISTRY_URL)

    private val consumer = KafkaConsumer<String, Fruit> (
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            AvroConstants.PROPERTY_SCHEMA_REGISTRY_URL to registryUrl
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
    val consumer = AvroConsumer("localhost", 9092, "fruits-consumer", "fruits")
    consumer.consumeForever()
}
