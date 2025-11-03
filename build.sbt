name := "ETL_Oracle_to_Hadoop_DataLake"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.10.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.mockito" %% "mockito-scala" % "1.5.12" % Test
)

fork in Test := true
