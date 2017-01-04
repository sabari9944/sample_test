package com.obs.test

import org.junit.Before
import org.junit.After
import org.junit.Test
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration
import java.io.{ FileOutputStream, File }
import com.obs.pn.acq.Acquisition
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.obs.pn.commons.Utils
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils
import org.junit.BeforeClass
import org.junit.Assert.assertEquals;
import java.net.URI


object AcquisitionTestIT {

  val logger = LoggerFactory.getLogger(DummyLogger.getClass)

  @BeforeClass
  def beforeC() {
    logger.info( " *******************")
    logger.info( " *** BEFORE CLASS ***")
    logger.info( " *******************")

    /** exec path */
    //current is /target
    val targetFolder = new File(".").getCanonicalPath

    System.setProperty("spark.local.dir", "temp")
    val os = System.getProperty("os.name").toLowerCase()

    if (os.indexOf("win") >= 0) {
      logger.info("This is a Windows environment")

      val tmpFile = new File("./tmp")
      if (tmpFile.exists())
        tmpFile.delete()

      //Runtime.getRuntime().exec("cmd /c mkdir tmp")
      tmpFile.mkdir()
      //works
      Runtime.getRuntime().exec("../../hadoop/win/bin/winutils.exe chmod 777 tmp")
      //works
      System.setProperty("hive.exec.scratchdir", targetFolder + "/tmp")
      //works
      logger.info("hadoop.home.dir=" + targetFolder + "/../../hadoop/win")
      System.setProperty("hadoop.home.dir", targetFolder + "/../../hadoop/win")
    } else {
      logger.info("This is NOT a Windows environment")
      System.setProperty("hadoop.home.dir", targetFolder + "/../../hadoop/unix")
    }

    //current is /target
    System.setProperty("obspn.application.conf", "../src/test/resources/application_dev.conf")

    System.setProperty("obspn.application.master", "local")
    System.setProperty("obspn.application.appname", "testapp")

    logger.info( " *******************")
  }

}

class AcquisitionTestIT {
  /** Logger instance*/
  val logger = LoggerFactory.getLogger(DummyLogger.getClass)

  @Test
  def loadAcort() {

    logger.info( " ***************************")
    logger.info( " *** EXECUTE LOAD ACCORT ***")
    logger.info( " ***************************")

    logger.debug( " Start of test")

    Acquisition.loadTimestampFile("dev.acort", "TEST1", false)

    logger.debug( " End of test")

    logger.info( " ***************************")

  }

  @Test
  def loadWasac() {

    logger.info( " **************************")
    logger.info( " *** EXECUTE LOAD WASAC ***")
    logger.info( " **************************")

    logger.debug( " Start of test")

    Acquisition.loadTimestampFile("dev.incidentologie_wasac", "TEST1", false)

    logger.debug( " End of test")

    logger.info( " ***************************")

  }

  @Test
  def loadCommunes() {

    logger.info( " *****************************")
    logger.info( " *** EXECUTE LOAD COMMUNES ***")
    logger.info( " *****************************")

    logger.debug( " Start of test")

    Acquisition.loadFile("dev.communes_INSEE_utile", true)

    logger.debug( " End of test")

    logger.info( " ***************************")

  }

  @Test
  def loadRapReference() {

    logger.info( " ****************************")
    logger.info( " *** EXECUTE LOAD RAP REF ***")
    logger.info( " ****************************")

    logger.debug( " Start of test")

    Acquisition.loadFile("dev.AllRapReference", true)

    logger.debug( " End of test")

    logger.info( " ****************************")

  }

  @Test
  def loadCorrespondanceInsee() {

    logger.info( " ******************************")
    logger.info( " *** EXECUTE LOAD COR INSEE ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")

    Acquisition.loadFile("dev.correspondance-code-insee-code-postal_utile", true)

    logger.debug( " End of test")

    logger.info( " ******************************")
    
  }
  
  @Test
  def addBlankCols() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE addBlankCols ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"nom_commune":"Michael","poulation_tot":"200000003","code_insee":"2335","nom_commune_caps":"versaille","nom_commune_caps2":"verdeau","code_postal":"9999"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)
    
    Utils.addBlankCols3(dffromjson)
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  
  @Test
  def loadRemoteConf() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE loadRemoteConf and propFileLoader***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val pathToConf = "../src/test/resources/application_dev.conf"
    
    Utils.loadLocalConf(pathToConf);
    Utils.loadRemoteConf(pathToConf);
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  @Test
  def getFileSystemByUri() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE getFileSystemByUri ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val pathToConf = "../src/test/resources/application_dev.conf"
    
    new URI(pathToConf)
    Utils.getFileSystemByUri(new URI(pathToConf));
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  } 
  
  @Test
  def udfReplaceST() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE udfReplaceST ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"ville_extremite_A":"ST"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)
    val df = Utils.udfReplaceST(dffromjson("ville_extremite_A"))
    
    val sqlContext1 = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events1 =Utils.sc.parallelize("""{"ville_extremite_A":"SAINT"}""" :: Nil)
    val dffromjson1 = sqlContext.read.json(events)
    val df1 = Utils.udfReplaceST(dffromjson("ville_extremite_A"))
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  @Test
  def calculateRap () {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE calculateRap  ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"type_interne_siSU":"Valirisation","type_interne_SI":"prestation"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)    
    val df = Utils.calculateCeId(dffromjson("type_interne_siSU"),dffromjson("type_interne_SI"))

    logger.debug( " Start of test")
    val events1 =Utils.sc.parallelize("""{"type_interne_siSU":"Valirisation","type_interne_SI":""}""" :: Nil)
    val dffromjson1 = sqlContext.read.json(events)    
    val df1 = Utils.calculateCeId(dffromjson("type_interne_siSU"),dffromjson("type_interne_SI"))

    logger.debug( " Start of test")
    val events2 =Utils.sc.parallelize("""{"type_interne_siSU":"","type_interne_SI":"Valirisation"}""" :: Nil)
    val dffromjson2 = sqlContext.read.json(events)    
    val df2 = Utils.calculateCeId(dffromjson("type_interne_siSU"),dffromjson("type_interne_SI"))
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  @Test
  def calculateRouterRole() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE calculateRouterRole  ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"type_interne":"Valirisation","role":"prestation","type_interne_siSU":"Valirisation"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)    
    val df = Utils.calculateRouterRole(dffromjson("type_interne"),dffromjson("role"))

    logger.debug( " Start of test")
    val events1 =Utils.sc.parallelize("""{"type_interne_siSU":"Valirisation","type_interne_SI":""}""" :: Nil)
    val dffromjson1 = sqlContext.read.json(events1)    
    val df1 = Utils.calculateCeId(dffromjson1("type_interne_siSU"),dffromjson1("type_interne_SI"))

    logger.debug( " Start of test")
    val events2 =Utils.sc.parallelize("""{"type_interne_siSU":"","type_interne_SI":"Valirisation"}""" :: Nil)
    val dffromjson2 = sqlContext.read.json(events2)    
    val df2 = Utils.calculateCeId(dffromjson2("type_interne_siSU"),dffromjson2("type_interne_SI"))
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  @Test
  def replaceStString() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE replaceStString  ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"ville_extremite_A":"ST","role":"prestation"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)    
    val df = Utils.replaceStString(dffromjson)
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
  
  @Test
  def selectOutputColumns() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE selectOutputColumns , suffix, dateDiffMin and getTimestamp ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"identifiant_1_produit":"azer","ip_admin":"azer","ios_version":"azer","constructeur":"azer","chassis":"azer","rsc":"azer","version_boot":"azer","num_pivot":"azer","element_reseau":"azer","type_interne":"azer","identifiant_eds_pilote":"azer","etat_produit":"azer","rap":"azer","libelle_rap":"azer","type_produit":"azer","datemescom":"azer","ce_id":"azer","connexion_id":"azer","support_id":"azer","service_support":"azer","collect":"azer","support":"azer","nb_paires":"azer","collect_role":"azer","router_role":"azer","gtr":"azer","plage_horaire_gtr":"azer","raison_sociale":"azer","raison_sociale_ticket":"azer","addresse_complete_client":"azer","code_postal_client":"azer","siren":"azer","societe_extremite_A":"azer","ville_extremite_A":"azer","voie_extremite_A":"azer","cp_extremite_A":"azer","societe_extremite_B":"azer","ville_extremite_B":"azer","voie_extremite_B":"azer","cp_extremite_B":"azer","population":"azer","population_cat":"azer","ios_version_from_iai":"azer","dslam_id":"azer","master_dslam_id":"azer","nortel_id":"azer","pe_id":"azer","fav_id":"azer","ntu_id":"azer","tronc_type":"azer"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)    
    val df = Utils.selectOutputColumns(dffromjson)
    val newString = Utils.suffix("ToTo")
    
    val events1 =Utils.sc.parallelize("""{"format":"","role":"prestation"}""" :: Nil)
    val dffromjson1 = sqlContext.read.json(events1)    
    val nullDate = Utils.getTimestamp(dffromjson1("format"))    
    val nullDate1 = Utils.getTimestamp(dffromjson1("format"))
    
    val events2 =Utils.sc.parallelize("""{"end":"1483106461","start":"1483146461"}""" :: Nil)
    val dffromjson2 = sqlContext.read.json(events2)    
    val nullDate2 = Utils.dateDiffMin(dffromjson2("end"),dffromjson2("start")) 
    val keepDF = Utils.keepDataFrame(dffromjson2)
    
    logger.debug( " End of test")

    logger.info(" ******************************")
  }
  
  @Test
  def testWriteFile() {
    
    logger.info( " ******************************")
    logger.info( " *** EXECUTE testWriteFile  ***")
    logger.info( " ******************************")

    logger.debug( " Start of test")
    val sqlContext = new org.apache.spark.sql.SQLContext(Utils.sc)
    val events =Utils.sc.parallelize("""{"ville_extremite_A":"ST","role":"prestation"}""" :: Nil)
    val dffromjson = sqlContext.read.json(events)
    new File("target/testFileHeader.test").delete();
    Acquisition.writeFile(dffromjson, "target/testFileHeader.test", true)
    new File("target/testFileNoHeader.test").delete();
    Acquisition.writeFile(dffromjson, "target/testFileNoHeader.test", false)
    
    logger.debug( " End of test")

    logger.info( " ******************************")
  
  }
}
