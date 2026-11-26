// The Tributary broker: a TCP server that stores topic partitions in the
// storage engine and answers produce, fetch and metadata requests.

plugins {
    application
    jacoco
}

application {
    mainClass = "io.github.karthika04.tributary.broker.BrokerMain"
    applicationName = "tributary-broker"
}

dependencies {
    implementation(project(":storage"))
    implementation(project(":protocol"))

    testImplementation(project(":client"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
