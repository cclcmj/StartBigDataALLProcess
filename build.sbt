ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "org.mj"
ThisBuild / version := "1.0"

lazy val root = (project in file(".")).settings(
  name := "start-all",
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "2.2.0",
    "org.scala-lang" % "scala-reflect" % "2.12.12"
  )
)