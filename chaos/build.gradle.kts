// Chaos testing: runs real 3-broker clusters as separate processes, injects faults
// (kill -9, network partitions, frozen processes, slow links) while producers write,
// and checks that nothing acknowledged was lost.

plugins {
    application
    jacoco
}

application {
    mainClass = "io.github.karthika04.tributary.chaos.ChaosRunner"
    applicationName = "chaos-runner"
}

dependencies {
    implementation(project(":client"))
    // The brokers run as child processes started from this module's class path,
    // so the broker (and, through it, storage for reading Raft logs) is needed here.
    implementation(project(":broker"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
