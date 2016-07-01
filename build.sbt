name := "sigopt-spark"

version := "0.0.1"

organization := "com.sigopt"
organizationName := "SigOpt"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6")

// Generate X-sources.jar
mappings in (Compile, packageSrc) ++= {
  val base  = (sourceManaged  in Compile).value
  val files = (managedSources in Compile).value
  files.map { f => (f, f.relativeTo(base).get.getPath) }
}

libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val v = scalaVersion match {
    case twoTen if scalaVersion.startsWith("2.10") => "_2.10"
    case twoEleven if scalaVersion.startsWith("2.11") => "_2.11"
    case _ => "_" + scalaVersion
  }
  Seq(
    "com.sigopt"                  %  ("sigopt-java") % "2.3.1",
    "org.apache.spark"            %  ("spark-core" + v) % "1.6.1",
    "org.apache.spark"            %  ("spark-mllib" + v) % "1.6.1"
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
