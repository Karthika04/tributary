// The `tributary` command-line tool: create topics, produce and consume
// messages, and inspect offsets from a terminal.

plugins {
    application
    jacoco
}

application {
    mainClass = "io.github.karthika04.tributary.cli.TributaryCli"
    applicationName = "tributary"
}

dependencies {
    implementation(project(":client"))

    testImplementation(project(":broker"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
