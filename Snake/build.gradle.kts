plugins {
    id("application")
    id("com.google.protobuf") version "0.9.4"
    id("org.openjfx.javafxplugin") version "0.1.0"
}

group = "org.example"
version = "1.0.0"

repositories {
    mavenCentral()
}

// Определяем платформу для JavaFX
val os = System.getProperty("os.name").lowercase()
val platform = when {
    os.contains("win") -> "win"
    os.contains("nix") || os.contains("nux") || os.contains("aix") -> "linux"
    os.contains("mac") -> "mac"
    else -> "linux"
}

dependencies {
    // JavaFX - platform-specific
    implementation("org.openjfx:javafx-controls:21:${platform}")
    implementation("org.openjfx:javafx-fxml:21:${platform}")
    implementation("org.openjfx:javafx-graphics:21:${platform}")
    implementation("org.openjfx:javafx-base:21:${platform}")

    // Protobuf
    implementation("com.google.protobuf:protobuf-java:3.25.1")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("org.slf4j:slf4j-api:2.0.9")
}

javafx {
    version = "21"
    modules = listOf("javafx.controls", "javafx.fxml")
}

application {
    mainClass.set("org.example.SnakesApplication")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.1"
    }

    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                // Плагин java добавляется автоматически
            }
        }
    }
}

sourceSets {
    main {
        proto {
            srcDir("src/main/proto")
        }
        java {
            srcDirs("build/generated/source/proto/main/java")
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

// Обработка дубликатов
tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Fat JAR
tasks.register<Jar>("fatJar") {
    archiveBaseName.set("snakes-game")
    archiveVersion.set("1.0.0")
    archiveClassifier.set("all")

    manifest {
        attributes(
            "Main-Class" to "org.example.SnakesApplication"
        )
    }

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.jar {
    manifest {
        attributes(
            "Main-Class" to "org.example.SnakesApplication"
        )
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Создание нативного приложения
tasks.register<Exec>("createApp") {
    dependsOn("build")

    val appName = "SnakeGame"
    val jarFile = "Snake-1.0.0.jar"
    val inputDir = file("build/libs")
    val outputDir = file("build/app")

    doFirst {
        outputDir.mkdirs()
    }

    commandLine(
        "jpackage",
        "--input", inputDir.absolutePath,
        "--name", appName,
        "--main-jar", jarFile,
        "--main-class", "org.example.SnakesApplication",
        "--type", "app-image",
        "--dest", outputDir.absolutePath,
        "--java-options", "--add-modules=javafx.controls,javafx.fxml",
        "--java-options", "--add-opens=javafx.graphics/com.sun.javafx.application=ALL-UNNAMED"
    )
}
