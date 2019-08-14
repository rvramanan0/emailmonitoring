
name := "emailmonitoring"

version := "0.1"

val sparkVersion = "2.0.1"




libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % "2.0.1" % Test)


libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.4"
libraryDependencies += "javax.mail" % "javax.mail-api" % "1.6.2"
libraryDependencies += "com.google.guava" % "guava" % "27.1-jre"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.5"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
//resolvers += "oracle" at "https://maven.oracle.com"
//credentials += Credentials("oracle download", "maven.oracle.com", "rvramanan055@gmail.com", "Prakrith@8")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.oracle" % "ojdbc" % "7" from "file:///C:/Users/756661/Downloads/Jars/ojdbc7.jar"


assemblyJarName in assembly := "emailassembly.jar"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
