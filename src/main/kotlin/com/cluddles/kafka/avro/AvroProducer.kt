package com.cluddles.kafka.avro

import avro.Thing
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

// Can view registered schemas, e.g. http://localhost:8081/schemas/ids/1
class AvroProducer(server: String, registryUrl: String) {

    constructor(host: String, port: Int): this("$host:$port", AvroConstants.REGISTRY_URL)

    private val producer: KafkaProducer<String, Thing> = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            "schema.registry.url" to registryUrl
        )
    )

    init {
        logger.info { "AvroProducer started, server: $server" }
    }

    fun produce(topic: String, thing: Thing) {
        logger.info { "SEND: $thing" }
        val send = producer.send(ProducerRecord(topic, thing))
        val recordMetadata = send.get()
        logger.debug { "recordMetadata: $recordMetadata" }
    }

    companion object {
        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val producer = AvroProducer("localhost", 9092)
    producer.produce("things", Thing("strawberry", "red"))
    producer.produce("things", Thing("blueberry", "blue"))
}
