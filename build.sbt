ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "com.example"
lazy val sparky_crawler = (project in file("."))
.settings(
name := "sparky-crawler",
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0",
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0",
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0",
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.2.1",
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.18"
)
