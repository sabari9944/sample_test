package com.obs.pn.test

import org.junit.Before
import org.junit.After
import org.junit.Test 
import org.junit.Assert.assertEquals
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration
import com.obs.pn.ticketenrichi.transf.Launcher
import java.io.{FileOutputStream, File}
import com.obs.pn.commons.Utils
import org.apache.commons.io.FileUtils
import com.obs.pn.ticketenrichi.util._

class MainTestIT
{
    /** Logger instance*/
    val logger = LoggerFactory.getLogger(DummyLogger.getClass)
    
    @Before
    def before() {
      
      val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
      logger.info(" configurationFilePath = "+configurationFilePath)
      
      logger.info(" *******************")
      logger.info(" *** BEFORE TEST ***")
      logger.info(" *******************")
         
      val os = System.getProperty("os.name").toLowerCase()
      
      if (os.indexOf("win") >= 0) {
        logger.info("This is a Windows environment")
        Runtime.getRuntime().exec("cmd /c cd ../../target & mkdir tmp")        
        Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe chmod 777 ../../target/tmp")
        System.setProperty("hive.exec.scratchdir", configurationFilePath + "../../../target/tmp")
        System.setProperty("hadoop.home.dir", configurationFilePath + "../../../hadoop/win")
      } else {
        System.setProperty("hadoop.home.dir", configurationFilePath + "../../../hadoop/unix")
      }
      val csvFile = new File("CSV/tickets_enrichis.csv")
      if (csvFile.exists())
        csvFile.delete()

      //Runtime.getRuntime().exec("cmd /c mkdir tmp")
      csvFile.mkdir()
      val tmpCsvFile = new File("../../src/test/ressources/referential/tickets_enrichis.csv")
      FileUtils.copyDirectory(tmpCsvFile, csvFile)
      System.setProperty("obspn.application.conf", configurationFilePath + "../../../application_dev.conf")
      System.setProperty("obspn.application.master", "local")
      System.setProperty("obspn.application.appname", "testapp")
      
      /** Set dataset run date */
      Utils.runDate = "20161128"
      
      logger.info(" *******************")
      
    }
    
    @After
    def after() {
      
      logger.info(" ******************")
      logger.info(" *** AFTER TEST ***")
      logger.info(" ******************")
            
      Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe rm -R  ../../../target/tmp")
      
      logger.info(" ******************")
      
    }
   
    @Test
    def ticketenrichi() {
      
      logger.info(" ******************************")
      logger.info(" *** EXECUTE TICKET ENRICHI ***")
      logger.info(" ******************************")
      
      logger.debug(" Start of ticketenrichi (first) ")
      
      Launcher.main(Array("TESTEmpty", System.getProperty("obspn.application.conf")))
      
      logger.debug(" End of ticketenrichi (first)")

      

    }
    
    @Test
  def correctNomCommune() {
    
    val stringTest :String = "ûûû-ûûûûûûûûû-ûûûûûûûûûûû-ûûû-ûûûûû-ûûûûûûû-ûûûûûûû"

    logger.info( " ****************************** ")
    logger.info( " *** EXECUTE correctNomCommune *** ")
    logger.info( " ****************************** ")

    logger.debug( " Start of test")
    
    logger.info( " Input string "+ stringTest)
    val result = CommunesInseeUtile.correctNomCommune(stringTest)
    assertEquals("ûûû ûûûûûûûûû ûûûûûûûûûûû ûûû ûûûûû ûûûûûûû ûûûûûûû", result)
    
    logger.info("Result strinng"+ result)
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  
  @Test
  def addCommunesCol() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE addCommunesCol replaceCommunesCol and transform ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"nom_commune":"Michael","poulation_tot":"200000003","code_insee":"2335","nom_commune_caps":"versaille","nom_commune_caps2":"verdeau"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)
    val commune = CommunesInseeUtile.transform(dffromjson)
    CommunesInseeUtile.replaceCommunesCol(dffromjson)
    CommunesInseeUtile.addCommunesCol(dffromjson)
    logger.debug( " End of test")

    logger.info( " ******************************")
  }
  
  @Test
  def CorrespondanceCodeInseecodePostalUtil() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE correctCodePostal and transform***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"nom_commune":"Michael","poulation_tot":"200000003","code_insee":"2335","nom_commune_caps":"versaille","nom_commune_caps2":"verdeau","code_postal":"9999"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)
    
    assertEquals("09999",CorrespondanceCodeInseecodePostalUtile.correctCodePostal("9999"))
    assertEquals("99999",CorrespondanceCodeInseecodePostalUtile.correctCodePostal("99999"))
    assertEquals("99",CorrespondanceCodeInseecodePostalUtile.correctCodePostal("99"))
    CorrespondanceCodeInseecodePostalUtile.transformCorrespondence(dffromjson)
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }

}
