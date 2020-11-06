ThisBuild / crossScalaVersions := Seq("2.12.11")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-mariadb"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-mariadb"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-mariadb"),
  "scm:git@github.com:precog/quasar-plugin-mariadb.git"))

ThisBuild / publishAsOSSProject := true

ThisBuild / githubWorkflowBuildMatrixAdditions +=
  "dbms" -> List("mariadb", "mysql") // TODO: memsql

ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(
    List("docker-compose up -d ${{ matrix.dbms }}"),
    name = Some("Start ${{ matrix.dbms }} container"))

val mariadbVersion = "2.6.0"
val specs2Version = "4.9.4"

lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

lazy val quasarPluginJdbcVersion =
  Def.setting[String](managedVersions.value("precog-quasar-plugin-jdbc"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core, destination, datasource)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-plugin-mariadb",

    libraryDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "com.codecommit" %% "cats-effect-testing-specs2" % "0.4.0" % Test,
      "org.specs2" %% "specs2-core" % specs2Version % Test
    ))

lazy val destination = project
  .in(file("destination"))
  .dependsOn(core % BothScopes)
  .settings(
    name := "quasar-destination-mariadb",

    quasarPluginName := "mariadb",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDestinationFqcn := Some("quasar.plugin.mariadb.destination.MariaDbDestinationModule$"),

    quasarPluginDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "org.mariadb.jdbc" % "mariadb-java-client" % mariadbVersion
    ),

    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Version % Test,
      "com.codecommit" %% "cats-effect-testing-specs2" % "0.4.0" % Test,
      "org.apache.logging.log4j" % "log4j-core" % "2.11.2" % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.2" % Test
    ))
  .enablePlugins(QuasarPlugin)

lazy val datasource = project
  .in(file("datasource"))
  .dependsOn(core % BothScopes)
  .settings(
    name := "quasar-datasource-mariadb",

    quasarPluginName := "mariadb",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDatasourceFqcn := Some("quasar.plugin.mariadb.datasource.MariaDbDatasourceModule$"),

    quasarPluginDependencies ++= Seq(
      "com.precog" %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "org.mariadb.jdbc" % "mariadb-java-client" % mariadbVersion
    ),

    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Version % Test,
      "com.codecommit" %% "cats-effect-testing-specs2" % "0.4.0" % Test,
      "org.apache.logging.log4j" % "log4j-core" % "2.11.2" % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.2" % Test
    ))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_PATH", "connector", true)
