rootProject.name = "tributary"

// Where Gradle downloads third-party libraries (JUnit, gRPC, ...) from.
dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("storage")
