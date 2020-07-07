ThisBuild / crossScalaVersions := Seq("2.12.11")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-mariadb"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-mariadb"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-mariadb"),
  "scm:git@github.com:precog/quasar-plugin-mariadb.git"))

ThisBuild / publishAsOSSProject := true

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-plugin-mariadb")
