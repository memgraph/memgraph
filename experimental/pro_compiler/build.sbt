
lazy val root = (project in file(".")).settings(
  name := "cypher-compiler",
  scalaVersion := "2.12.1",
  version := "0.1",
  libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5"
  )
)
