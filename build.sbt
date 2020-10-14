lazy val `parquet-experiment-root` = project
  .in(file("."))
  .aggregate(`parquet-experiment`)

lazy val `parquet-experiment` = project
  .settings(
    scalaVersion := "2.13.3",
    resolvers += "jitpack" at "https://jitpack.io",
    libraryDependencies ++= Seq(
      "com.github.tmtsoftware.csw" %% "csw-event-client"         % "04eaee2",
      "com.github.mjakubowski84"   %% "parquet4s-akka"           % "1.5.1",
      "org.apache.hadoop"           % "hadoop-client"            % "3.3.0",
      "com.lightbend.akka"         %% "akka-stream-alpakka-file" % "2.0.2"
    )
  )

lazy val `delta-writer` = project
  .settings(
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "io.delta"          %% "delta-core"        % "0.7.0",
      "org.apache.spark"  %% "spark-sql"         % "3.0.1",
      "com.typesafe.akka" %% "akka-stream-typed" % "2.6.9",
      "io.bullet"         %% "borer-derivation"  % "1.6.2"
    )
  )
