# sigopt-spark

Compiles against the Spark 1.6.1 build on scala 2.10 and 2.11.

sigopt-spark integrates SigOpt's functionality into Spark for performing Bayesian
optimization over hyperparameters. You can use this as a drop-in replacement for
CrossValidator. Just provide your SigOpt client token and the number of iterations
you'd like to use.

## Installation

### Maven users

Update your project settings (commonly found at `~/.m2/settings.xml`) to include the SigOpt package.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<settings xsi:schemaLocation='http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd'
xmlns='http://maven.apache.org/SETTINGS/1.0.0' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>
  <profiles>
    <profile>
      <repositories>
        <repository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>bintray-sigopt-maven</id>
          <name>bintray</name>
          <url>http://dl.bintray.com/sigopt/maven</url>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>bintray-sigopt-maven</id>
          <name>bintray-plugins</name>
          <url>http://dl.bintray.com/sigopt/maven</url>
        </pluginRepository>
      </pluginRepositories>
      <id>bintray</id>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>bintray</activeProfile>
  </activeProfiles>
</settings>
```

More information on this step can be found on [BinTray](https://bintray.com/sigopt/maven/sigopt-spark_2.10/view)

Scala 2.10 users: Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>com.sigopt</groupId>
  <artifactId>sigopt-spark_2.10</artifactId>
  <version>1.0.0</version>
</dependency>
```

Scala 2.11 users: Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>com.sigopt</groupId>
  <artifactId>sigopt-spark_2.11</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Others

You'll need to manually install the following JARs:

* Apache Spark version 1.6.1
* The sigopt-spark JAR.

## Example Usage

Check out our [Scala Example][../examples/LinearRegressionCV.scala].

More examples available in our [../examples][examples section].
