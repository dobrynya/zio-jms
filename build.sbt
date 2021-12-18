name := "zio-jms"
organization := "io.github.dobrynya"
version := "0.2"
homepage := Some(url("https://github.com/dobrynya/zio-jms"))
developers += Developer("dobrynya", "Dmitry Dobrynin", "dobrynya@inbox.ru", url("https://github.com/dobrynya"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/dobrynya/zio-jms"),
    "scm:git@github.com:dobrynya/zio-jms.git"
  )
)
licenses += ("APACHE2.0", url("https://opensource.org/licenses/Apache-2.0"))
ThisBuild / versionScheme := Some("early-semver")
crossPaths := true
publishMavenStyle := true
publishTo := Some("releases" at "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2")

scalaVersion := "3.1.2"
crossScalaVersions := List("2.13.7", "3.1.2")

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "2.0.0-RC6",
  "dev.zio" %% "zio-streams" % "2.0.0-RC6",
  "dev.zio" %% "zio-test" % "2.0.0-RC6" % Test,
  "dev.zio" %% "zio-test-sbt" % "2.0.0-RC6" % Test,
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1" % Provided,
  "org.apache.activemq" % "activemq-broker" % "5.16.2" % Test,
  "org.apache.activemq" % "activemq-kahadb-store" % "5.16.2" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.6" % Test,
  "io.github.sullis" %% "jms-testkit" % "1.0.4" % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
