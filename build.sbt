libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "org.specs2" %% "specs2" % "2.3.12" % "test"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
