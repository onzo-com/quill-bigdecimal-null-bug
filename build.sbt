name := "quill-bigdecimal-null-bug"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.2",
  "io.getquill" %% "quill-cassandra" % sys.env.getOrElse("QUILL_CASSANDRA_VERSION", "3.4.9"),
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)
