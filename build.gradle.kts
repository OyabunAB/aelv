import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.signing)
    alias(libs.plugins.dokka)
    alias(libs.plugins.nexusPublish)
    alias(libs.plugins.jmh)
}

version = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"

sourceSets {
    create("tck") {
        compileClasspath += sourceSets.main.get().output + configurations.runtimeClasspath.get()
        runtimeClasspath += sourceSets.main.get().output + configurations.runtimeClasspath.get()
    }
}

dependencies {
    api(libs.bundles.core.api)
    implementation(libs.bundles.core)

    testImplementation(libs.bundles.test)

    "tckImplementation"(libs.bundles.tck)

    "jmhImplementation"(libs.bundles.jmh.benchmarks)
    "jmhAnnotationProcessor"(libs.jmh.generator.annprocess)
}

version = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"

val isPublishable = !version.toString().endsWith("-SNAPSHOT")
val isRelease     = Regex("""^\d+\.\d+\.\d+$""").matches(version.toString())
val signingKey: String? = System.getenv("GPG_SIGNING_KEY")
val signingPassword: String? = System.getenv("GPG_SIGNING_PASSWORD")

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
}

val target = JvmTarget.fromTarget(libs.versions.jvm.get())
tasks.compileKotlin { compilerOptions { jvmTarget.set(target) } }
tasks.compileTestKotlin { compilerOptions { jvmTarget.set(target) } }

val tckTest by tasks.registering(Test::class) {
    description = "Runs Reactive Streams TCK compliance tests."
    group = "verification"
    testClassesDirs = sourceSets["tck"].output.classesDirs
    classpath = sourceSets["tck"].runtimeClasspath
    useTestNG {
        suites("src/tck/resources/tck-suite.xml")
    }
    failOnNoDiscoveredTests = false
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

tasks.check { dependsOn(tckTest) }

jmh {
    warmupIterations = 2
    iterations = 3
    fork = 1
    timeUnit = "ms"
    benchmarkMode = listOf("thrpt")
    resultFormat = "JSON"
    resultsFile = project.file("build/reports/jmh/results.json")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("started", "passed", "skipped", "failed")
        showStandardStreams = true
    }
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier = "sources"
    from(sourceSets.main.map { it.allSource })
}

val javadocJar = tasks.register<Jar>("javadocJar") {
    dependsOn(tasks.dokkaGenerate)
    archiveClassifier = "javadoc"
    from(layout.buildDirectory.dir("dokka/html"))
}

if (isPublishable && signingKey != null) {
    signing {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications)
    }
}

if (isRelease) {
    nexusPublishing {
        repositories {
            sonatype {
                username.set(System.getenv("OSSRH_USERNAME"))
                password.set(System.getenv("OSSRH_PASSWORD"))
            }
        }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OyabunAB/aelv")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
            pom {
                name = "aelv"
                description = "Minimalistic reactive streams implementation for Kotlin."
                url = "https://github.com/OyabunAB/aelv"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/OyabunAB/aelv.git"
                    developerConnection = "scm:git:ssh://github.com:OyabunAB/aelv.git"
                    url = "https://github.com/OyabunAB/aelv"
                }
                developers {
                    developer {
                        id = "dansun"
                        name = "Daniel Sundberg"
                        email = "daniel.sundberg@oyabun.se"
                    }
                }
            }
        }
    }
}
