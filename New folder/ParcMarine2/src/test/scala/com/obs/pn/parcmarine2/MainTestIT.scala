package com.obs.pn.parcmarine2

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

class MainTestIT {
  /** Logger instance*/
  val logger = LoggerFactory.getLogger(DummyLogger.getClass)

  @Before
  def before() {

    val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
    logger.info(System.currentTimeMillis() + " configurationFilePath = " + configurationFilePath);

    logger.info(System.currentTimeMillis() + " *******************");
    logger.info(System.currentTimeMillis() + " *** BEFORE TEST ***");
    logger.info(System.currentTimeMillis() + " *******************");

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
    Utils.runDate = "TEST"
    //Utils.runDate = "20161128"

    logger.info(System.currentTimeMillis() + " *******************");

  }
  @After
  def after() {

    logger.info(System.currentTimeMillis() + " ******************");
    logger.info(System.currentTimeMillis() + " *** AFTER TEST ***");
    logger.info(System.currentTimeMillis() + " ******************");

    Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe rm -R  ../../../target/tmp");

    logger.info(System.currentTimeMillis() + " ******************");

  }

  @Test
  def parcmarine2() {

    logger.info(System.currentTimeMillis() + " ********************************")
    logger.info(System.currentTimeMillis() + " *** EXECUTE TEST ParcMarine2 ***")
    logger.info(System.currentTimeMillis() + " ********************************")

    logger.debug(System.currentTimeMillis() + " Start of Launcher.parcmarine2()");

    Launcher.parcmarine2();

    logger.debug(System.currentTimeMillis() + " End of Launcher.parcmarine2()");

  }

  /** Reads the output of the parcmarine_2 output */
  @Test
  def parcmarine_2_output_read() {

    logger.info(System.currentTimeMillis() + " ********************************")
    logger.info(System.currentTimeMillis() + " *** EXECUTE TEST ParcMarine2 ***")
    logger.info(System.currentTimeMillis() + " ********************************")

    logger.debug(System.currentTimeMillis() + " Start of Launcher.parcmarine2()");

    val reader = Utils.sqlContext.read.format("com.databricks.spark.csv");

    var test_df = null: DataFrame

    test_df = reader.load("../../target/parquet_read.csv")

    println(test_df.show)

    logger.debug(System.currentTimeMillis() + " End of Launcher.parcmarine2()");

  }

}
