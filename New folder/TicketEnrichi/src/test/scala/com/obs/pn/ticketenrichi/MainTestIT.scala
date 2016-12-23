package com.obs.pn.ticketenrichi

import org.junit.Before;
import org.junit.After;
import org.junit.Test; 
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import com.obs.pn.ticketenrichi.transf.Launcher;
import java.io.{FileOutputStream, File};
import com.obs.pn.commons.Utils;
import com.obs.pn.ticketenrichi.DummyLogger;
import org.apache.commons.io.FileUtils;

class MainTestIT
{
    /** Logger instance*/
    val logger = LoggerFactory.getLogger(DummyLogger.getClass)
    
    @Before
    def before() {
      
      val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
      logger.info(System.currentTimeMillis() +" configurationFilePath = "+configurationFilePath);
      
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
      
      /** Set dataset run date */
      Utils.runDate = "TEST"
      
      logger.info(System.currentTimeMillis() +" *******************");
      
    }
    @After
    def after() {
      
      logger.info(System.currentTimeMillis() +" ******************");
      logger.info(System.currentTimeMillis() +" *** AFTER TEST ***");
      logger.info(System.currentTimeMillis() +" ******************");
            
      Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe rm -R  ../../../target/tmp");
      
      logger.info(System.currentTimeMillis() +" ******************");
      
    }
   
    @Test
    def ticketenrichi() {
      
      logger.info(System.currentTimeMillis() +" ******************************")
      logger.info(System.currentTimeMillis() +" *** EXECUTE TICKET ENRICHI ***")
      logger.info(System.currentTimeMillis() +" ******************************")
      
      logger.debug(System.currentTimeMillis() +" Start of ticketenrichi ");
      
      Launcher.ticketenrichi();
      
      logger.debug(System.currentTimeMillis() +" End of ticketenrichi");
      
      
      
    }

}
