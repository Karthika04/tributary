// Raft consensus, implemented from scratch: leader election, log replication and
// commit rules. Knows nothing about topics or sockets; the broker plugs in a network
// transport and a state machine.

plugins {
    `java-library`
    jacoco
}

dependencies {
    implementation(project(":storage"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
