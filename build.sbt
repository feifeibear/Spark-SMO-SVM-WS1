//name := "spark-smo"

//version := "0.1"

//scalaVersion := "2.10.3"

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := "2.10.3"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    // your settings here
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",

	libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0" % "provided",

	libraryDependencies += "amplab" % "spark-indexedrdd" % "0.3",

	resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",

	resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"
  )

  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
