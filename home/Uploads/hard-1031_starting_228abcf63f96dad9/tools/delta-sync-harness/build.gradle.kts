plugins {
    kotlin("jvm") version "2.2.20"
    kotlin("plugin.serialization") version "2.2.20"
    id("app.cash.sqldelight") version "2.3.2"
}
dependencies {
    implementation("app.cash.sqldelight:async-extensions:2.3.2")
    implementation("app.cash.sqldelight:coroutines-extensions:2.3.2")
    implementation("app.cash.sqldelight:sqlite-driver:2.3.2")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.9.0")
    implementation("com.squareup.okhttp3:okhttp:5.1.0")
    implementation("io.reactivex:rxjava:1.3.8")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
}
sqldelight {
    databases {
        create("Database") {
            packageName.set("tachiyomi.data")
            dialect("app.cash.sqldelight:sqlite-3-38-dialect:2.3.2")
            generateAsync.set(true)
            srcDirs("src/main/sqldelight")
        }
    }
}
kotlin {
    jvmToolchain(21)
    compilerOptions { optIn.addAll("kotlinx.serialization.ExperimentalSerializationApi", "kotlin.concurrent.atomics.ExperimentalAtomicApi") }
}
tasks.test { useJUnit() }
