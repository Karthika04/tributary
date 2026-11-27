// Benchmarks.
//  - tributary-perf: a load generator measuring throughput and latency percentiles
//    against real brokers, like Kafka's producer-perf-test.
//  - JMH micro-benchmarks of the storage log, the wire codec and the Raft command
//    encoder:  ./gradlew :bench:jmh   (filter with -PjmhArgs="Commands")

plugins {
    application
}

application {
    mainClass = "io.github.karthika04.tributary.bench.PerfMain"
    applicationName = "tributary-perf"
}

dependencies {
    implementation(project(":client"))
    // Starts brokers as separate processes (single broker, or a 3-broker cluster).
    implementation(project(":chaos"))
    // CommandsBenchmark sits in the broker's package to reach the package-private Commands.
    implementation(project(":broker"))
    // Accurate latency percentiles (p99, p99.9) in fixed memory.
    implementation(libs.hdrhistogram)

    // JMH, used directly rather than through a Gradle plugin: the annotation processor
    // generates the benchmark harness at compile time, and the jmh task runs JMH's Main.
    implementation(libs.jmh.core)
    annotationProcessor(libs.jmh.generator.annprocess)
}

// JMH's generated code is not ours to lint.
tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.removeAll(listOf("-Xlint:all"))
}

tasks.register<JavaExec>("jmh") {
    group = "benchmark"
    description = "Runs the JMH micro-benchmarks. Pass JMH options with -PjmhArgs=\"...\"."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.openjdk.jmh.Main"
    val resultFile = layout.buildDirectory.file("jmh/results.json").get().asFile
    doFirst { resultFile.parentFile.mkdirs() }
    val extra = (project.findProperty("jmhArgs") as String?)?.split(" ")?.filter { it.isNotBlank() } ?: emptyList()
    args = extra + listOf("-rf", "json", "-rff", resultFile.path)
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
