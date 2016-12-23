package com.obs.pn.ticketenrichi.commons

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.net.URI
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext

import com.obs.pn.ticketenrichi.logging.CommonLogger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import java.io.File
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import parquet.org.slf4j.LoggerFactory

object Utils {

  val logger = LoggerFactory.getLogger(classOf[CommonLogger])

  val conf = new SparkConf().setAppName("TicketEnrichIs")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  private val HDFS_IMPL_KEY = "fs.hdfs.impl"
  var prop = ConfigFactory.load()
  var runDate = "": String
  var pathToConf = "": String

  /**
   * Config Loader from parameter passed as argument
   */
  def loadConf(pathToConf: String): Config = {
    val path = new Path(pathToConf)
    val confFile = File.createTempFile(path.getName, "tmp")
    confFile.deleteOnExit()
    getFileSystemByUri(path.toUri).copyToLocalFile(path, new Path(confFile.getAbsolutePath))
    ConfigFactory.load(ConfigFactory.parseFile(confFile))
  }

  def getFileSystemByUri(uri: URI): FileSystem = {
    val hdfsConf = new Configuration()
    hdfsConf.set(HDFS_IMPL_KEY, classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    FileSystem.get(uri, hdfsConf)
  }
  /**
   * File loader with extension and date
   */
  def propFileLoader(fileName: String): String = {
    var fileNm: String = prop.getString(fileName + ".dir") + runDate + prop.getString(fileName + ".file") + runDate + prop.getString("dev.fileExtension")
    return fileNm
  }

  /**
   * UDF to generate service Support
   */
  def closeContext() {
    sc.stop()
  }

  /**
   * UDF to generate service Support
   */
  def getContext(): HiveContext = {
    return sqlContext
  }

  /**
   * UDF to generate service Support
   */
  val generateServiceSupport = udf((router_role: String, support_type_nominal: String, support_type_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      support_type_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      support_type_secours
    else null)

  /**
   * UDF to generate CollecteNominal
   */
  val generateCollecteNominal = udf((router_role: String, collecte_nominal: String, collecte_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      collecte_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      collecte_secours
    else null)

  /**
   * UDF to generate support
   */
  val udfSupport = udf((router_role: String, support_nominal: String, support_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      support_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      support_secours
    else null)

  /**
   * UDF to replace string ST to Saint
   */
  val udfReplaceST = udf((input: String) =>
    if (input != null && input.startsWith("ST "))
      input.replace("ST ", "SAINT ")
    else input)

  /**
   * This udf returns blank value. Used to create column with blank values.
   */
  val createNewBlankColumn = udf((type_interne: String) => None: Option[String])

  /**
   * UDF to generate "rap" column value
   */
  val calculateRap = udf((type_interne_siSU: String, type_interne_SI: String) =>
    if (type_interne_siSU != null && type_interne_siSU.length() >= 10) type_interne_siSU.substring(3, 10)
    else if (type_interne_SI != null) type_interne_SI
    else null)

  /**
   * UDF to generate ce_id
   */
  val calculateCeId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && element_reseau != null && type_interne.indexOf("RSS_TYP160") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP160"))
    else if (type_interne != null && type_interne.indexOf("R_TRL") != -1)
      element_reseau(type_interne.indexOf("R_TRL"))
    else null)
    


  /**
   * UDF to generate connexion_id
   */
  val calculateConnexionId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("RSS_TYP164") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP164"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP180") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP180"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP220") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP220"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP210") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP210"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP165") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP165"))
    else if (type_interne != null && type_interne.indexOf("A_AFR") != -1)
      element_reseau(type_interne.indexOf("A_AFR"))
    else if (type_interne != null && type_interne.indexOf("A_IAP") != -1)
      element_reseau(type_interne.indexOf("A_IAP"))
    else if (type_interne != null && type_interne.indexOf("A_PHD") != -1)
      element_reseau(type_interne.indexOf("A_PHD"))
    else null)

  /**
   * UDF to generate support_id
   */
  val calculateSupportId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("RSS_TYP163") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP163"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP178") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP178"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP209") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP209"))
    else if (type_interne != null && type_interne.indexOf("RSS_TYP180") != -1)
      element_reseau(type_interne.indexOf("RSS_TYP180"))
    else null)

  /**
   * UDF to generate router_role
   */
  val calculateRouterRole = udf((type_interne: String, role: String) =>
    if (type_interne != null && "RSS_TYP160".equals(type_interne)) role
    else null)

  /**
   * UDF to generate dslam_id
   */
  val calculateDslamId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_DSLAM") != -1)
      element_reseau(type_interne.indexOf("TOPO_DSLAM"))
    else null)

  /**
   * UDF to generate nortel_id
   */
  val calculateNortelId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_NORTEL") != -1)
      element_reseau(type_interne.indexOf("TOPO_NORTEL"))
    else null)

  /**
   * Generate pe_id
   */
  val calculatePeId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_PE") != -1)
      element_reseau(type_interne.indexOf("TOPO_PE"))
    else null)

  /**
   * UDF to generate fav_id
   */
  val calculateFavId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_FAV") != -1)
      element_reseau(type_interne.indexOf("TOPO_FAV"))
    else null)

  /**
   * UDF to generate ntu_id
   */
  val calculateNtuId = udf((type_interne: Seq[String], element_reseau: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_NTU") != -1)
      element_reseau(type_interne.indexOf("TOPO_NTU"))
    else null)

  /**
   * UDF to generate tronc_type
   */
  val calculateTroncType = udf((type_interne: Seq[String]) =>
    if (type_interne != null && type_interne.indexOf("TOPO_TRONC") != -1) "TOPO_TRONC"
    else null)

  /**
   * UDF for creating quoted field
   */
  val createColumnWithQuote = udf((type_interne: String) => "\"")

  /**
   * Joins gatherInput and AllRapReference
   */
  def joinGatherAndAllRapRef(gatherInput: DataFrame, AllRapReference: DataFrame): DataFrame =
    {
      try {
        val res = gatherInput.join(AllRapReference,
          gatherInput("rap") === AllRapReference("rap"), "left_outer")
          .drop(AllRapReference("rap"))

        logger.info("joinGatherAndAllRapRef transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          gatherInput
        }
      }
    }

  /**
   * Add field ServiceSupport
   */
  def addServiceSupport(joinGatherAllRapRef: DataFrame): DataFrame =
    {
      try {
        val res = joinGatherAllRapRef.withColumn("service_support",
          generateServiceSupport(joinGatherAllRapRef("router_role"),
            joinGatherAllRapRef("support_type_nominal"),
            joinGatherAllRapRef("support_type_secours")))

        logger.info("addServiceSupport transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          joinGatherAllRapRef
        }
      }

    }

  /**
   * Add field ServiceSupport
   */
  def addCollectNominal(transform1: DataFrame): DataFrame =
    {
      try {
        val res = transform1.withColumn("collect",
          generateCollecteNominal(transform1("router_role"),
            transform1("collecte_nominal"),
            transform1("collecte_secours")))

        logger.info("addCollectNominal transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          transform1
        }
      }
    }

  /**
   * Add field Support
   */
  def addSupport(transform2: DataFrame): DataFrame =
    {
      try {
        val res = transform2.withColumn("support",
          udfSupport(transform2("router_role"),
            transform2("support_nominal"),
            transform2("support_secours")))
        logger.info("addSupport transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          transform2
        }
      }

    }

  /**
   * Replace string ST with Saint
   */
  def replaceStString(transform3: DataFrame): DataFrame =
    {
      try {
        val res = transform3.withColumn("ville_extremite_A_new",
          udfReplaceST(transform3("ville_extremite_A")))
          .drop(transform3("ville_extremite_A"))
        logger.info("addSupport transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          transform3
        }
      }
    }

  /**
   * Rename column
   */
  def renameColumnVilleExtremiteA(transformReplace: DataFrame): DataFrame =
    {
      try {
        val res = transformReplace.withColumnRenamed("ville_extremite_A_new",
          "ville_extremite_A")
        logger.info("renameColumnVilleExtremiteA transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          transformReplace
        }
      }
    }

  /**
   * Drop column
   */
  def dropColumnLibelleRap(deselect: DataFrame): DataFrame =
    {
      try {
        val res = deselect.drop("libelle_rap")
        logger.info("dropColumnLibelleRap transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          deselect
        }
      }
    }

  /**
   * Make IOS field blank
   */
  def changeIOSField(wasac: DataFrame): DataFrame =
    {
      try {
        val res = wasac.na.replace("ios", Map("\n" -> ""))
        logger.info("changeIOSField transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          wasac
        }
      }
    }

  /**
   * Join Wasac And Acort files
   */
  def joinWasacAndAcort(sortedWasacSkippedNewline: DataFrame, dfAcort: DataFrame): DataFrame =
    {
      try {
        val res = sortedWasacSkippedNewline.join(dfAcort,
          sortedWasacSkippedNewline("feuillet") === dfAcort("element_reseau"),
          "left_outer")
        logger.info("changeIOSField transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          sortedWasacSkippedNewline
        }
      }
    }

  /**
   * <<<<<<< .mine
   * =======
   * Add blank column
   */
  def addBlankCols1(deselectJoin: DataFrame): DataFrame =
    {
      try {
        val res = deselectJoin.withColumn("gtr", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("plage_horaire_gtr", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("raison_sociale", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("raison_sociale_ticket", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("addresse_complete_client", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("code_postal_client", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("siren", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("societe_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ville_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("voie_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("cp_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("societe_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ville_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("voie_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("cp_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("population", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("population_cat", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ios_version_from_iai", Utils.createNewBlankColumn(lit("": String)))

        logger.info("addBlankCols1 transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          deselectJoin
        }
      }
    }

  /**
   * Add blank column
   */
  def addBlankCols2(deselectJoin: DataFrame): DataFrame =
    {
      try {
        val res = deselectJoin.withColumn("identifiant_eds_pilote", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("etat_produit", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("libelle_rap", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("type_produit", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("datemescom", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("service_support", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("collect", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("support", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("nb_paires", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("collect_role", Utils.createNewBlankColumn(lit("": String)))

        logger.info("addBlankCols2 transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          deselectJoin
        }
      }
    }

  /**
   * Add blank column
   */
  def addBlankCols3(select: DataFrame): DataFrame =
    {
      try {
        val res = select.withColumn("gtr", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("plage_horaire_gtr", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("raison_sociale", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("raison_sociale_ticket", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("addresse_complete_client", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("code_postal_client", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("siren", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("societe_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ville_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("voie_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("cp_extremite_A", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("societe_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ville_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("voie_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("cp_extremite_B", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("population", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("population_cat", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ios_version_from_iai", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("identifiant_eds_pilote", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("etat_produit", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("libelle_rap", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("type_produit", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("datemescom", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("service_support", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("collect", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("support", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("nb_paires", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("collect_role", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("master_dslam_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("rap", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ce_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("connexion_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("support_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("router_role", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("dslam_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("nortel_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("pe_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("fav_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("ntu_id", Utils.createNewBlankColumn(lit("": String)))
          .withColumn("tronc_type", Utils.createNewBlankColumn(lit("": String)))

        logger.info("addBlankCols3 transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          select
        }
      }
    }

  /**
   * Select related columns
   */
  def selectOutputColumns(deselectJoin: DataFrame): DataFrame =
    {
      try {
        val res = deselectJoin.select("identifiant_1_produit", "ip_admin", "ios_version", "constructeur", "chassis",
          "rsc", "version_boot", "num_pivot", "element_reseau", "type_interne",
          "identifiant_eds_pilote", "etat_produit", "rap", "libelle_rap", "type_produit",
          "datemescom", "ce_id", "connexion_id", "support_id", "service_support",
          "collect", "support", "nb_paires", "collect_role", "router_role",
          "gtr", "plage_horaire_gtr", "raison_sociale", "raison_sociale_ticket", "addresse_complete_client",
          "code_postal_client", "siren", "societe_extremite_A", "ville_extremite_A", "voie_extremite_A",
          "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B",
          "population", "population_cat", "ios_version_from_iai", "dslam_id", "master_dslam_id",
          "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type")

        logger.info("changeIOSField transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          deselectJoin
        }
      }
    }

  /**
   * >>>>>>> .r349
   * Function to suffix value in IncidentologieWasacIai file
   */
  def suffix(s: String): String =
    {
      if (s != null) s.takeRight(1) else null
    }

  val getTimestamp = udf((x: String) => {
    val format = new SimpleDateFormat("yyyyMMddhhmmss")
    if ((x.toString() == "") || (x.toString() == "null") || (x.toString() == null)) null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      t
    }
  })

  val dateDiffMin = udf((end: Timestamp, start: Timestamp) =>
    {
      if (end != null && start != null)
        ((end.getTime - start.getTime) / (60 * 1000)) else 0;
    })

}
