name := "zio-jms"
version := "0.1"
scalaVersion := "2.13.2"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "1.0.0-RC21",
  "dev.zio" %% "zio-streams" % "1.0.0-RC21",
  "dev.zio" %% "zio-test" % "1.0.0-RC21" % Test,
  "dev.zio" %% "zio-test-sbt" % "1.0.0-RC21" % Test,
  "dev.zio" %% "zio-test-junit" % "1.0.0-RC21" % Test,
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1",
  "org.apache.activemq" % "activemq-broker" % "5.15.12" % Test,
  "org.apache.activemq" % "activemq-kahadb-store" % "5.15.12" % Test,
  "ch.qos.logback" % "logback-classic" % "[1.2,)" % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
