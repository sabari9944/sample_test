lazy val commonSettings = Seq(

 	version := "G00R00C02",	
	scalaVersion := "2.10.6",
	//logLevel := Level.Debug,

	libraryDependencies ++=Seq(
	  "org.apache.spark" %% "spark-core" % "1.5.2",
	  "org.apache.spark" % "spark-sql_2.10" % "1.5.2",
	  "com.databricks" % "spark-csv_2.10" % "1.5.0",
	  "commons-dbutils" % "commons-dbutils" % "1.5" % "test",
	  "org.apache.spark" % "spark-hive_2.10" % "1.5.2",
	  "com.typesafe" % "config" % "1.2.1"  % "test",
	  "commons-dbutils" % "commons-dbutils" % "1.5"  % "test" ,
	  "org.scalatest" %% "scalatest" % "3.0.0"  % "test",
	  "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.2_0.3.3" % "test"
	 )
	 
)

lazy val Obs_pn_ParcMarine2 = (project in file("./ParcMarine2")).
  settings(commonSettings: _*).
  settings(packAutoSettings: _*) 

  
lazy val Obs_Pn_TicketEnrichi = (project in file("./TicketEnrichi")).
  settings(commonSettings: _*).
  settings(packAutoSettings: _*)
	 
  
lazy val Obs_Pn_Common = (project in file("./CommonPN")).
  settings(commonSettings: _*).
  settings(packAutoSettings: _*)
	
  