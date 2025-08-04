plugins {
    application
    kotlin("jvm") version "2.0.0"
}

group = "org.etux"
version = "1.0-SNAPSHOT"

application {
    mainClass = "org.etux.MainKt"
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(11)
}

java {
    version = JavaVersion.VERSION_11
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:4.0.0")
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("org.slf4j:slf4j-simple:2.0.17")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.0")
    testImplementation(kotlin("test"))
    testImplementation("org.assertj:assertj-core:3.27.3")
}

tasks.test {
    useJUnitPlatform()
}