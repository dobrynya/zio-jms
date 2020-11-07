name := "zio-jms"
organization := "com.gh.dobrynya"
version := "0.1"
scalaVersion := "2.13.3"
crossScalaVersions := Seq("2.12.12")
licenses += ("APACHE2.0", url("https://opensource.org/licenses/Apache-2.0"))

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "[1.0.0,)",
  "dev.zio" %% "zio-streams" % "[1.0.0,)",
  "dev.zio" %% "zio-test" % "[1.0.0,)" % Test,
  "dev.zio" %% "zio-test-sbt" % "[1.0.0,)" % Test,
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1",
  "org.apache.activemq" % "activemq-broker" % "5.15.12" % Test,
  "org.apache.activemq" % "activemq-kahadb-store" % "5.15.12" % Test,
  "ch.qos.logback" % "logback-classic" % "[1.2,)" % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
