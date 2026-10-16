ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "junit" % "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test
    ),
    Test / testFrameworks += new TestFramework("com.novocode.junit.JUnitFramework")
  )
Test / fork := true

Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
)

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
)

libraryDependencies +=
  "org.postgresql" % "postgresql" % "42.7.7"