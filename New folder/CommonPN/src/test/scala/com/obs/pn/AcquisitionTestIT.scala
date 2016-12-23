package com.obs.pn

import org.junit.Before; 
import org.junit.After;
import org.junit.Test; 
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import java.io.{FileOutputStream, File};
import com.obs.pn.acq.Acquisition;
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import com.obs.pn.DummyLogger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.obs.pn.commons.Utils
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils;

class AcquisitionTestIT
{
    /** Logger instance*/
    val logger = LoggerFactory.getLogger(DummyLogger.getClass)
    
    /** exec path */
    val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
    logger.info(System.currentTimeMillis() +" configurationFilePath = "+configurationFilePath);
      
    @Before
    def before() {
      
      logger.info(System.currentTimeMillis() +" *******************");
      logger.info(System.currentTimeMillis() +" *** BEFORE TEST ***");
      logger.info(System.currentTimeMillis() +" *******************");
         
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
      
      logger.info(System.currentTimeMillis() +" *******************");
      
    }
    
    @After
    def after() {
      
      logger.info(System.currentTimeMillis() +" ******************");
      logger.info(System.currentTimeMillis() +" *** AFTER TEST ***");
      logger.info(System.currentTimeMillis() +" ******************");
         
      Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe rm -R  ../../../target/tmp");
      
      logger.info(System.currentTimeMillis() +" *******************");
      
    }
   
    @Test
    def loadAcort() {
      
      logger.info(System.currentTimeMillis() +" ***************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE LOAD ACCORT ***")
      logger.info(System.currentTimeMillis() +" ***************************")
      
      logger.debug(System.currentTimeMillis() +" Start of test");
      
      Acquisition.loadTimestampFile( "dev.acort", "TEST", false);
      
      logger.debug(System.currentTimeMillis() +" End of test");
      
      logger.info(System.currentTimeMillis() +" ***************************");
      
    }
    
    @Test
    def loadWasac() {
      
      logger.info(System.currentTimeMillis() +" **************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE LOAD WASAC ***")
      logger.info(System.currentTimeMillis() +" **************************")
      
      logger.debug(System.currentTimeMillis() +" Start of test");
      
      Acquisition.loadTimestampFile( "dev.wasac", "TEST", false);
      
      logger.debug(System.currentTimeMillis() +" End of test");
      
      logger.info(System.currentTimeMillis() +" ***************************");
      
    }
    
    @Test
    def loadCommunes() {
      
      logger.info(System.currentTimeMillis() +" *****************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE LOAD COMMUNES ***")
      logger.info(System.currentTimeMillis() +" *****************************")
      
      logger.debug(System.currentTimeMillis() +" Start of test");
      
      Acquisition.loadFile( "dev.communes",  true);
      
      logger.debug(System.currentTimeMillis() +" End of test");
      
      logger.info(System.currentTimeMillis() +" ***************************");
      
    }
    
    @Test
    def loadRapReference() {
      
      logger.info(System.currentTimeMillis() +" ****************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE LOAD RAP REF ***")
      logger.info(System.currentTimeMillis() +" ****************************")
      
      logger.debug(System.currentTimeMillis() +" Start of test");
      
      Acquisition.loadFile( "dev.rapReference",  true);
      
      logger.debug(System.currentTimeMillis() +" End of test");
      
      logger.info(System.currentTimeMillis() +" ****************************");
      
    }
    
    @Test
    def loadCorrespondanceInsee() {
      
      logger.info(System.currentTimeMillis() +" ******************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE LOAD COR INSEE ***")
      logger.info(System.currentTimeMillis() +" ******************************")
      
      logger.debug(System.currentTimeMillis() +" Start of test");
      
      Acquisition.loadFile( "dev.correspondance_insee",  true);
      
      logger.debug(System.currentTimeMillis() +" End of test");
      
      logger.info(System.currentTimeMillis() +" ******************************");
      
    }

}
