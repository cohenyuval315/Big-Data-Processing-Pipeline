
// The simplest possible sbt build file is just one line:

// scalaVersion := "2.13.8"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings". Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.13.8"

// It's possible to define many kinds of settings, such as:

// name := "kafka_transformations"
// organization := "ch.epfl.scala"
// version := "1.0"

// Note, it's not required for you to define these three settings. These are
// mostly only necessary if you intend to publish your library's binaries on a
// place like Sonatype.


name := "kafka_transforms"
version := "1.0"
scalaVersion := "2.13.8"

val kafka_prefix = "org.apache.kafka"
val kafka = "kafka"
val kafka_streams = "kafka-streams"
val kafka_streams_scala = "kafka-streams-scala"
val kafka_streams_test = "kafka-streams-test-utils"
val kafka_connect_transform = "connect-transforms"
val kafka_connect_json = "connect-json"
val kafka_connect_api = "connect-api"
val kafka_hadoop_consumer = "kafka-hadoop-consumer"
val kafka_hadoop_producer = "kafka-hadoop-producer"
val kafka_log4j = "kafka-log4j-appender"
val kafka_metadata = "kafka-metadata"
val kafkaVersion = "3.5.1"

val kafkaLibraryDependencies = Seq(
    kafka_prefix %% kafka % kafkaVersion,
    kafka_prefix % kafka_streams % kafkaVersion,
    kafka_prefix %% kafka_streams_scala % kafkaVersion,
    kafka_prefix % kafka_streams_test % kafkaVersion,
    kafka_prefix % kafka_connect_transform % kafkaVersion,
    kafka_prefix % kafka_connect_json % kafkaVersion,
    kafka_prefix % kafka_connect_api % kafkaVersion,
    // kafka_prefix % kafka_hadoop_consumer % "0.8.2.2",
    // kafka_prefix % kafka_hadoop_producer % "0.8.2.2",
)
val otherLibraryDependencies = Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"
)

libraryDependencies ++= kafkaLibraryDependencies
libraryDependencies ++= otherLibraryDependencies

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.8",
//       version := "1.0"
//     )),
//     name := "kafka_transformations",
//     //libraryDependencies ++= dataDependencies,
//     //libraryDependencies ++= sparkDependencies,
//     libraryDependencies ++= kafkaLibraryDependencies,
//     libraryDependencies ++= otherLibraryDependencies,
//     //libraryDependencies ++= Seq(gigahorse, playJson),
//   )


// Here, `libraryDependencies` is a set of dependencies, and by using `+=`,
// we're adding the scala-parser-combinators dependency to the set of dependencies
// that sbt will go and fetch when it starts up.
// Now, in any Scala file, you can import classes, objects, etc., from
// scala-parser-combinators with a regular import.

// TIP: To find the "dependency" that you need to add to the
// `libraryDependencies` set, which in the above example looks like this:

// "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

// You can use Scaladex, an index of all known published Scala libraries. There,
// after you find the library you want, you can just copy/paste the dependency
// information that you need into your build file. For example, on the
// scala/scala-parser-combinators Scaladex page,
// https://index.scala-lang.org/scala/scala-parser-combinators, you can copy/paste
// the sbt dependency from the sbt box on the right-hand side of the screen.

// IMPORTANT NOTE: while build files look _kind of_ like regular Scala, it's
// important to note that syntax in *.sbt files doesn't always behave like
// regular Scala. For example, notice in this build file that it's not required
// to put our settings into an enclosing object or class. Always remember that
// sbt is a bit different, semantically, than vanilla Scala.

// ============================================================================

// Most moderately interesting Scala projects don't make use of the very simple
// build file style (called "bare style") used in this build.sbt file. Most
// intermediate Scala projects make use of so-called "multi-project" builds. A
// multi-project build makes it possible to have different folders which sbt can
// be configured differently for. That is, you may wish to have different
// dependencies or different testing frameworks defined for different parts of
// your codebase. Multi-project builds make this possible.

// Here's a quick glimpse of what a multi-project build looks like for this
// build, with only one "subproject" defined, called `root`:

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.8"
//     )),
//     name := "hello-world"
//   )

// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html
import sbt._
mainClass in Compile := Some("Main")