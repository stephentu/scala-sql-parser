libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "org.specs2" %% "specs2" % "1.8.2" % "test"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
