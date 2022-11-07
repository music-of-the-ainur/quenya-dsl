ThisBuild / name := "quenya-dsl"
ThisBuild / organization := "com.github.music-of-the-ainur"

lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.9"

crossScalaVersions := Seq(scala212,scala213)
ThisBuild / scalaVersion := scala212

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test"
)

enablePlugins(GitVersioning)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/music-of-the-ainur/quenya-dsl"),
    "scm:git@github.com:music-of-the-ainur/quenya-dsl.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "mantovani",
    name  = "Daniel Mantovani",
    email = "daniel.mantovani@modakanalytics.com",
    url   = url("https://github.com/music-of-the-ainur")
  ),
  Developer(
    id = "ChinthapallyAkanksha",
    name = "Akanksha Chinthapally",
    email = "akanksha.chinthapally@modak.com",
    url = url("https://github.com/music-of-the-ainur")
  )
)

ThisBuild / description := "Quenya DSL(Domain Specific Language) is a language that simplifies the task to parser complex semi-structured data"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/music-of-the-ainur/quenya-dsl"))

// Remove all additional repository other than Maven Central from POM
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true
updateOptions := updateOptions.value.withGigahorse(false)
