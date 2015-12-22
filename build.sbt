resolvers += Resolver.sonatypeRepo("public")

name := "mlx-intrusion-detection"

version := "0.1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
//  "org.json4s" %% "json4s-jackson" % "3.2.9" % "provided",
  "org.apache.spark" %% "spark-core" % "1.4.1"  % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.1"  % "provided"
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}


