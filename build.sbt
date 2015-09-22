val companyName = "aggregations"
val domain = "sam"

val mavenRepo: String = "maven." + companyName + "." + domain

resolvers ++= Seq(
  "mvnrepository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test" withSources() withJavadoc(),
  "org.specs2" %% "specs2-core" % "2.4.15" % "test" withSources() withJavadoc(),
  "org.specs2" %% "specs2-scalacheck" % "2.4.15" % "test" withSources() withJavadoc(),
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  // For some reason omitting spark-sql causes crazy exceptions ... *tut* *tut* typical
  ("org.apache.spark" % "spark-sql_2.10" % "1.3.0-cdh5.4.2") withSources() withJavadoc(),
  ("org.apache.spark" % "spark-core_2.10" % "1.3.0-cdh5.4.2") withSources() withJavadoc(),
  ("org.apache.spark" % "spark-mllib_2.10" % "1.3.0-cdh5.4.2") withSources() withJavadoc()
)


// Strat copied from defaultMergeStrategy with the "fail and confuse the hell out the user" lines changed to
// "just bloody work and stop pissing everyone off"
mergeStrategy in assembly <<= (mergeStrategy in assembly)((old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    (xs map {
      _.toLowerCase
    }) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first // Changed deduplicate to first
    }
  case PathList(_*) => MergeStrategy.first // added this line
})

scalaVersion := "2.10.4"

//scalacOptions ++= Seq("-deprecation", "-feature")

javaOptions ++= Seq("-target", "1.8", "-source", "1.8")

organization := domain + "." + companyName

name := "aggregations"

parallelExecution in Test := false

version := "0.1.0"
