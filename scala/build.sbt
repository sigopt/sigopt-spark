name := "sigopt-spark"

version := "2.0.0"

organization := "com.sigopt"
organizationName := "SigOpt"

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.11.12")

// Generate X-sources.jar
mappings in (Compile, packageSrc) ++= {
  val base  = (sourceManaged  in Compile).value
  val files = (managedSources in Compile).value
  files.map { f => (f, f.relativeTo(base).get.getPath) }
}

libraryDependencies ++= Seq(
  "com.sigopt"        %  ("sigopt-java")     % "6.2.0",
  "org.apache.spark"  %%  ("spark-core")     % "2.4.4",
  "org.apache.spark"  %%  ("spark-mllib")    % "2.4.4",
  "org.json4s"        %%  ("json4s-jackson") % "3.6.7",
)

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
