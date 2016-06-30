name := "sigopt-spark"

version := "0.0.1"

organization := "com.sigopt"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

libraryDependencies <++= (scalaVersion) { scalaVersion =>
  Seq(
    "com.sigopt"                  %  ("sigopt-java") % "2.3.1",
    "org.apache.spark"            %  "spark-core_2.10" % "1.6.1",
    "org.apache.spark"            %  "spark-mllib_2.10" % "1.6.1"
  )
}

pomIncludeRepository := { x => false }

pomExtra := (
<url>https://github.com/sigopt/sigopt-spark</url>
<licenses>
  <license>
    <name>Apache 2</name>
    <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    <distribution>repo</distribution>
    <comments>A business-friendly OSS license</comments>
  </license>
</licenses>
<scm>
  <url>git@github.com/sigopt/sigopt-spark.git</url>
  <connection>scm:git:git@github.com/sigopt/sigopt-spark.git</connection>
</scm>
<developers>
  <developer>
    <id>patrick</id>
    <name>Patrick Hayes</name>
    <email>patrick@sigopt.com</email>
  </developer>
</developers>
)
