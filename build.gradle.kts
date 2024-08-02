plugins {
    kotlin("jvm") version "2.0.0"
}

group = "com.cluddles"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api(libs.kotlin.logging)
    api(libs.log4j.core)
    api(libs.log4j.slf4j2)
    api(libs.kafka.clients)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

tasks.register<JavaExec>("simpleConsumer") {
    description = "Run the SimpleConsumer"
    group = "run"
    mainClass.set("com.cluddles.kafka.SimpleConsumerKt")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("simpleProducer") {
    description = "Run the SimpleProducer"
    group = "run"
    mainClass.set("com.cluddles.kafka.SimpleProducerKt")
    classpath = sourceSets["main"].runtimeClasspath
}
