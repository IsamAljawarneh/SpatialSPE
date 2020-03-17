ThisBuild / scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "SSS",
   libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "harsha2010" % "magellan" % "1.0.5-s_2.11",
    "com.esri.geometry" % "esri-geometry-api" % "2.2.1"
    ),
compileOrder := CompileOrder.JavaThenScala,
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
  )

resolvers ++= Seq(
    "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
    "jitpack" at "https://jitpack.io",
    "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
