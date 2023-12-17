ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)
lazy val root = (project in file("."))
  .settings(
    name := "yazan_assignment"
  )
