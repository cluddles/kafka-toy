package com.cluddles.kafka.avro

import avro.Fruit
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

// Can view registered schemas, e.g. http://localhost:8081/schemas/ids/1
class AvroProducer(server: String, registryUrl: String) {

    constructor(host: String, port: Int): this("$host:$port", AvroConstants.REGISTRY_URL)

    private val producer: KafkaProducer<String, Fruit> = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            AvroConstants.PROPERTY_SCHEMA_REGISTRY_URL to registryUrl
        )
    )

    init {
        logger.info { "AvroProducer started, server: $server" }
    }

    fun produce(topic: String, fruit: Fruit) {
        logger.info { "SEND: $fruit" }
        val send = producer.send(ProducerRecord(topic, fruit))
        val recordMetadata = send.get()
        logger.debug { "recordMetadata: $recordMetadata" }
    }

    companion object {
        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val producer = AvroProducer("localhost", 9092)
    producer.produce("fruits", Fruit("strawberry", "red"))
    producer.produce("fruits", Fruit("blueberry", "blue"))
}
