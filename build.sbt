import sbt.Defaults.testSettings
import sbt.Keys.name
import sbtassembly.AssemblyKeys.assemblyMergeStrategy
import sbtassembly.{MergeStrategy, PathList}

name := "kafka-snow-white"

lazy val core = withIntegrationTests {
  project
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= (coreDependencies ++ coreTestDependencies))
}

lazy val `app-common` = withIntegrationTests {
  project
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= (consulAppDependencies ++ consulAppTestDependencies))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      buildInfoKeys := List[BuildInfoKey](
        name,
        version,
        scalaVersion,
        sbtVersion,
        "gitCommit" -> git.gitHeadCommit.value.getOrElse("")),
      buildInfoPackage := organization.value)
    .settings(resolvers += Resolver.jcenterRepo)
    .dependsOn(core % "compile -> compile; test -> test; it -> it")
}

lazy val `consul-app` = withIntegrationTests {
  project
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= (consulAppDependencies ++ consulAppTestDependencies))
    .settings(resolvers += Resolver.jcenterRepo)
    .dependsOn(`app-common` % "compile -> compile; test -> test; it -> it")
}

lazy val `file-watcher-app` = withIntegrationTests {
  project
    .settings(baseSettings: _*)
    .settings(libraryDependencies ++= fileAppDependencies)
    .dependsOn(`app-common` % "compile -> compile; test -> test; it -> it")
}

def baseSettings = List(
  organization := "com.supersonic",
  version := "0.1",
  scalaVersion := "2.12.4",
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
  scalacOptions.in(Compile, console) ~= filterConsoleScalacOptions,
  scalacOptions.in(Test, console) ~= filterConsoleScalacOptions,
  sources in (Compile, doc) := List.empty,
  mergeStrategy,
  test in assembly := List(
    (test in Test).value,
    (test in IntegrationConfig).value),
  addCompilerPlugin("io.tryp" % "splain" % "0.2.9" cross CrossVersion.patch))

def filterConsoleScalacOptions = { options: Seq[String] =>
  options.filterNot(Set("-Xlint", "-Xfatal-warnings"))
}

/** Same as [[IntegrationTest]] but with an additional dependency on [[Test]], so that they can
  * share code.
  */
val IntegrationConfig = IntegrationTest.extend(Test)

def withIntegrationTests(project: Project) = {

  val testWithIntegration =
    test in Test := (test in IntegrationConfig).dependsOn(test in Test).value

  val integrationTestSettings =
    inConfig(IntegrationConfig)(testSettings) ++
      Seq(
        testWithIntegration,
        parallelExecution in IntegrationConfig := false)

  project
    .settings(integrationTestSettings)
    .configs(IntegrationConfig.extend(Test))
}

val akkaVersion = "2.5.7"
val akkaHTTPVersion = "10.0.11"

def consulAkkaStream = "com.supersonic" %% "consul-akka-stream" % "1.0.3"

def coreDependencies = List(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  "com.iheart" %% "ficus" % "1.4.3",
  "org.typelevel" %% "cats-core" % "1.0.1") ++ loggingDependencies

def consulAppDependencies = List(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  consulAkkaStream,
  "org.typelevel" %% "cats-core" % "1.0.1",
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.github.fommil" %% "spray-json-shapeless" % "1.4.0")

def fileAppDependencies =
  List("com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.18")

def coreTestDependencies = List(
  "org.scalatest" %% "scalatest" % "3.0.4" % "it, test",
  "com.ironcorelabs" %% "cats-scalatest" % "2.3.1" % "it, test",
  "com.softwaremill.quicklens" %% "quicklens" % "1.4.11" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it, test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "it, test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.16.0" % "it",
  // should probably be last, due to classpath magic, I think, maybe...
  // bridges logging with the embedded Kafka instance
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % "it")

def consulAppTestDependencies = List(consulAkkaStream % "it" classifier "it")

def loggingDependencies = List( //TODO where should these be used?
  // the order of the logging libraries matters, since they're loaded by classpath magic
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "me.moocar" % "logback-gelf" % "0.12")

def mergeStrategy =
  assemblyMergeStrategy in assembly := {
    //    case PathList("logback-test.xml") => MergeStrategy.last //TODO needed?
    case PathList("org", "apache", "commons", "collections", _*) => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
