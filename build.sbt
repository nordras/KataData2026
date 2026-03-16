ThisBuild / version := "0.2.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val flinkVersion = "1.19.1"

lazy val root = (project in file("."))
  .settings(
    name := "katadata-flink-scala",
    Compile / run / mainClass := Some("com.katadata.jobs.SalesAggregationsJob"),
    Compile / run / fork := true,
    Test / fork := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" % "flink-streaming-java" % flinkVersion,
      "org.apache.flink" % "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
      "org.apache.flink" % "flink-table-runtime" % flinkVersion,
      "org.apache.flink" % "flink-table-planner-loader" % flinkVersion,
      "org.apache.flink" % "flink-connector-jdbc" % "3.2.0-1.19",
      "org.slf4j" % "slf4j-simple" % "1.7.36",
      "org.apache.derby" % "derby" % "10.16.1.1"
    )
  )
