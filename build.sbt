import sbt.Defaults.testSettings
import sbt.Keys.name
import sbtassembly.AssemblyKeys.assemblyMergeStrategy
import sbtassembly.{MergeStrategy, PathList}

name := "kafka-snow-white"

lazy val core = withIntegrationTests {
  project
    .settings(name := "kafka-snow-white-core")
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= (coreDependencies ++ coreTestDependencies))
}

lazy val `app-common` = withIntegrationTests {
  project
    .settings(name := "kafka-snow-white-app-common")
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= appCommonDependencies)
    .enablePlugins(BuildInfoPlugin)
    .settings(
      buildInfoKeys := List[BuildInfoKey](
        name,
        version,
        scalaVersion,
        sbtVersion,
        "gitCommit" -> git.gitHeadCommit.value.getOrElse(""),
        "gitDescribedVersion" -> git.gitDescribedVersion.value.getOrElse("")),
      buildInfoPackage := organization.value)
    .dependsOn(core % "compile -> compile; test -> test; it -> it")
}

lazy val `consul-app` =
  withAssemblyArtifact {
    withIntegrationTests {
      project
        .settings(name := "kafka-snow-white-consul-app")
        .settings(baseSettings: _*)
        .settings(libraryDependencies ++= (consulAppDependencies ++ consulAppTestDependencies))
        .settings(Compile / run / mainClass := Some("com.supersonic.main.KafkaConsulMirrorApp"))
        .dependsOn(`app-common` % "compile -> compile; test -> test; it -> it")
    }
  }

lazy val `file-watcher-app` =
  withAssemblyArtifact {
    withIntegrationTests {
      project
        .settings(name := "kafka-snow-white-file-watcher-app")
        .settings(baseSettings: _*)
        .settings(libraryDependencies ++= fileAppDependencies)
        .settings(Compile / run / mainClass := Some("com.supersonic.main.KafkaFileWatcherMirrorApp"))
        .dependsOn(`app-common` % "compile -> compile; test -> test; it -> it")
    }
  }

sonatypeCredentialHost := Sonatype.sonatype01
inThisBuild(List(
  organization := "com.supersonic",
  homepage := Some(url("https://github.com/SupersonicAds/kafka-snow-white")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(Developer("SupersonicAds", "SupersonicAds", "SupersonicAds", url("https://github.com/SupersonicAds"))),

  githubWorkflowPublishTargetBranches := Seq(RefPredicate.StartsWith(Ref.Tag("v"))),
  githubWorkflowTargetTags ++= Seq("v*"),
  githubWorkflowPublish := Seq(
    WorkflowStep.Sbt(
      List("ci-release"),
      env = Map(
        "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
        "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
        "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
        "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}")))))

def baseSettings = List(
  scalaVersion := "2.12.8",
  scalacOptions ++= List(
    "-encoding", "UTF-8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds",
    "-Xfatal-warnings",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-Xlint",
    "-Ypartial-unification",
    "-P:splain:color:false"),
  sonatypeCredentialHost := Sonatype.sonatype01,
  resolvers += Resolver.jcenterRepo,
  Compile / console / scalacOptions ~= filterConsoleScalacOptions,
  Test / console / scalacOptions ~= filterConsoleScalacOptions,
  Compile / doc / sources := List.empty,
  mergeStrategy,
  // a workaround for https://github.com/sbt/sbt/issues/1380
  makePomConfiguration := makePomConfiguration.value.withConfigurations(Configurations.defaultMavenConfigurations),
  addCompilerPlugin("io.tryp" % "splain" % "0.5.8" cross CrossVersion.patch))

val akkaVersion = "2.5.21"
val akkaHTTPVersion = "10.1.8"

def coreDependencies = List(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.1",
  "com.iheart" %% "ficus" % "1.4.5",
  "org.typelevel" %% "cats-core" % "1.6.0") ++ loggingDependencies

def appCommonDependencies = List(
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.github.fommil" %% "spray-json-shapeless" % "1.4.0")

val consulAkkaStreamVersion = "1.1.1"

def consulAppDependencies = List(
  "com.supersonic" %% "consul-akka-stream" % consulAkkaStreamVersion)

def fileAppDependencies =
  List(
    "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.20",
    "com.google.guava" % "guava" % "19.0")

def coreTestDependencies = List(
  "org.scalatest" %% "scalatest" % "3.0.5" % "it, test",
  "com.ironcorelabs" %% "cats-scalatest" % "2.4.0" % "it, test",
  "com.softwaremill.quicklens" %% "quicklens" % "1.4.11" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it, test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "it, test",
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.2.0" % "it",
  // should probably be last, due to classpath magic, I think, maybe...
  // bridges logging with the embedded Kafka instance
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % "it")

def consulAppTestDependencies = List(
  "com.supersonic" %% "consul-akka-stream-integration-tests" % consulAkkaStreamVersion % "it")

def loggingDependencies = List( //TODO where should these be used?
  // the order of the logging libraries matters, since they're loaded by classpath magic
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "me.moocar" % "logback-gelf" % "0.12")

def filterConsoleScalacOptions = { options: Seq[String] =>
  options.filterNot(Set("-Xlint", "-Xfatal-warnings"))
}

/** Same as [[IntegrationTest]] but with an additional dependency on [[Test]], so that they can
  * share code.
  */
val IntegrationConfig = IntegrationTest.extend(Test)

/** Configures a project to support integration tests using the [[IntegrationTest]] configuration. */
def withIntegrationTests(project: Project) = {

  val testWithIntegration =
    Test / test  := (IntegrationConfig / test).dependsOn(Test / test).value

  val integrationTestSettings =
    inConfig(IntegrationConfig)(testSettings) ++
      Seq(
        testWithIntegration,
        IntegrationConfig / parallelExecution := false)

  project
    .settings(integrationTestSettings)
    .configs(IntegrationConfig.extend(Test))
}

/** Configures a project to publish its assembly JAR as part of the published artifacts. */
def withAssemblyArtifact(project: Project) =
  project
    .settings {
      Compile / assembly / artifact ~= { art =>
        art.withClassifier(Some("assembly"))

      }
    }
    .settings(addArtifact(Compile / assembly / artifact, assembly).settings: _*)
    .settings(assembly / test := {})


def mergeStrategy =
  assembly / assemblyMergeStrategy := {
    case PathList("org", "apache", "commons", "collections", _*) => MergeStrategy.last
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
