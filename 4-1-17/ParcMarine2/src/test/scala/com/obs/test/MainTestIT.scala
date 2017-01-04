package com.obs.test

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import com.obs.pn.parcmarine2.transf.Launcher;
import java.io.{ FileOutputStream, File };
import com.obs.pn.commons.Utils;
import com.obs.pn.acq.Acquisition
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode 

class MainTestIT {
  /** Logger instance*/
  val logger = LoggerFactory.getLogger(DummyLogger.getClass)

  @Before
  def before() {

     val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
    logger.info(" configurationFilePath = " + configurationFilePath);
    logger.info("*** BEFORE TEST ***");
    val os = System.getProperty("os.name").toLowerCase();

    if (os.indexOf("win") >= 0) {
      logger.info("This is a Windows environment")
      Runtime.getRuntime().exec("cmd /c cd ../../target & mkdir tmp")
      Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe chmod 777 ../../target/tmp")
      System.setProperty("hive.exec.scratchdir", configurationFilePath + "../../../target/tmp")
      System.setProperty("hadoop.home.dir", configurationFilePath + "../../../hadoop/win")
    } else {
      System.setProperty("hadoop.home.dir", configurationFilePath + "../../../hadoop/unix")
    }

    System.setProperty("obspn.application.conf", configurationFilePath + "../../../application_dev.conf");
    System.setProperty("obspn.application.master", "local");
    
    System.setProperty("obspn.application.appname", "testapp");

    /** Set dataset run date */
    Utils.runDate = "TESTEmpty"
    //Utils.runDate = "20161128"
  }
  @After
  def after() {

    logger.info("*** AFTER TEST ***");
    Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe rm -R  ../../../target/tmp");
  }

  @Test
  def parcmarine2() {
    /** Launching the parcMarine 2 Launcher */
    logger.debug("### Start of Launcher.main() ###");
    Launcher.main(Array("TESTEmpty", System.getProperty("obspn.application.conf")))
    logger.debug("### End of Launcher.main() ###");
  }

  /** Reads the output of the parcmarine_2 output */
  //@Test
  def parcmarine_2_output_read() {

    logger.info("### Reading last Dataframe Output ParcMarine 2 ###");

    /** Load SparkContext */
    val reader = Utils.sqlContext.read.format("com.databricks.spark.csv");

    /** New dataFrame */
    var test_df = null: DataFrame

    /** Reading DataFrame */
    test_df = reader.load("../../target/parquet_read.csv")

    /** Printing a few lines of the DataFrame */
    println(test_df.show)
    
    /** Saving as CSV */
    logger.info("Saving as CSV")
    
    /** gathering all the data on one Worker */  
    test_df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("../../target/mydata.csv")

    logger.info("### Reading ParcMarine 2 completed ###");
  }
}
