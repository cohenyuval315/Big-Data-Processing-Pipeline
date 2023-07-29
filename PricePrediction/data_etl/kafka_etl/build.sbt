name := "scala_project"
version := "1.0"
scalaVersion := "2.13.8"

// lazy val root = (project in file("."))
//   .aggregate(kafka_transforms)

lazy val kafka_transforms = project.in(file("project/kafka_transforms"))

import sbt._
mainClass in Compile := Some("Main")