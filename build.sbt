ThisBuild / name := "quenya-dsl"
ThisBuild / organization := "com.github.music.of.the.ainur.quenya"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"

crossScalaVersions := Seq(scala211, scala212)
ThisBuild / scalaVersion := scala212

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(GitVersioning)
