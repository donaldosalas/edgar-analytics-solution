
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.solution",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "EdgarAnalyticsSolution",
    libraryDependencies ++= Seq(
      "com.github.tototoshi" %% "scala-csv" % "1.3.5",
      "joda-time" % "joda-time" % "2.9.7",
      "org.joda" % "joda-convert" % "1.2",
    )
  )
