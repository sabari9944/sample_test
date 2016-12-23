package com.obs.pn.acq

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.StructType
import com.obs.pn.commons.Utils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

object Acquisition {
  
  /** Logger */
  val logger = LoggerFactory.getLogger(Acquisition.getClass)
  
  /**
   * Constants for loading of files
   */
  val FORMAT = "com.databricks.spark.csv"
  
  /**
   * Option to specify the delimiter of csv file
   */
  val DELIMITER = "delimiter" 
  
  /**
   * Option in databricks to specify while reading or writing the file the header to be included or not
   */
  val HEADER = "header"
  
  /**
   * To infer column types
   */
  val INFERSCHEMA = "inferSchema"
  
  /**
   * Delimiter value while loading file
   */
  val SEMICOLON = ";"
  
  /**
   * True value
   */
  val TRUE = "true"
  
  /**
   * Library for csv parsing
   * 
   */
  val PARSERLIB = "parserLib"
  
  /**
   * value to the library used for csv parsing.
  */
  val UNIVOCITY = "univocity"
  
  /**
   * Charset while reading the file.
   */
  val CHARSET = "charset"
  
  /**
   * Charset value. Default UTF-8
   */
  val CHARSETVALUE = "iso-8859-15"
  
  /**
   * get file name suffix by timestamp from property
   */
  def getTimestampFilename(property: String, timestamp: String): String = {
    
    logger.debug(System.currentTimeMillis() + " Property : " + property)        
    val filename: String = Utils.prop.getString(property + ".dir") + timestamp + Utils.prop.getString(property + ".file") + timestamp + Utils.prop.getString("dev.fileExtension")
    logger.debug(System.currentTimeMillis() + " Filename : " + filename)        
    filename
  }
  
  /**
   * get file name suffix from property
   */
  def getFilename(property: String): String = {
    
    logger.debug(System.currentTimeMillis() + " Property : " + property)        
    val filename: String = Utils.prop.getString(property + ".dir") + Utils.prop.getString(property + ".file") + Utils.prop.getString("dev.fileExtension")
    logger.debug(System.currentTimeMillis() + " Filename : " + filename)        
    filename
  }
  
  /**
   * load timestamp file content from property
   */
  def loadTimestampFile( property: String, timestamp: String, univocity: Boolean): DataFrame = {
    val res = loadDataFile(getTimestampFilename( property, timestamp), univocity)
    res
  }
  
  /**
   * load timestamp file content from property
   */
  def loadFile( property: String, univocity: Boolean): DataFrame = {
    val res = loadDataFile(getFilename( property), univocity)
    res
  }
  
  /**
   * load data file from filename (HDFS directory)
   */
  def loadDataFile(fileName: String, univocity: Boolean): DataFrame =
    {
      try {
        
        logger.info(System.currentTimeMillis() + " Start loading : " + fileName)
        
        val reader = Utils.sqlContext.read
            .format(Acquisition.FORMAT);
          
        if( univocity ) {
          reader.option(Acquisition.PARSERLIB, Acquisition.UNIVOCITY)
            .option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
            .option(Acquisition.INFERSCHEMA, Acquisition.TRUE) // Automatically infer data types
            .option(Acquisition.HEADER, Acquisition.TRUE) // Use first line of all files as header
            .option(Acquisition.CHARSET, Acquisition.CHARSETVALUE)
              
        } else {
          reader.option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
            .option(Acquisition.INFERSCHEMA, Acquisition.TRUE)
            .option(Acquisition.HEADER, Acquisition.TRUE) 
        }

        val res = reader.load(fileName)
        
        logger.info(System.currentTimeMillis() + " End loading : " + fileName + " with " + res.count() + " lines")
        
        res
      } catch {
        case e: Exception => {
          logger.error(System.currentTimeMillis() + " Unable to load file " + fileName, e)
          Utils.sqlContext.emptyDataFrame
        }
      }
    }
  
    /**
     * Write files to given location.
     */
    def writeFile(dataframe: DataFrame, fileName: String): Unit =
      {
        try {
          dataframe.write.format(Acquisition.FORMAT)
            .option(Acquisition.HEADER, Acquisition.TRUE)
            .option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
            .mode(SaveMode.Overwrite)
            .save(fileName)
  
          logger.debug(System.currentTimeMillis() + "-" + fileName + " write completed successfully"  )
  
        } catch {
          case e: Exception => {
            logger.error("Failed to write file "+fileName, e)
            throw e
          }
        }
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
     * UDF to generate "rap" column value from typeSI Array
     */
  val calculateRap = udf((typeSI: Seq[String], type_interne: Seq[String]) =>
    if (typeSI != null && typeSI.indexOf("S-SU") != -1 && type_interne(typeSI.indexOf("S-SU")).length() >=10) 
          type_interne(typeSI.indexOf("S-SU")).substring(3, 10)
    else if (typeSI != null && typeSI.indexOf("A-Z") != -1) 
      type_interne(typeSI.indexOf(typeSI.startsWith("A-Z")))
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

        logger.debug(System.currentTimeMillis() + " join Gather Input andA llRapReference completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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

        logger.debug(System.currentTimeMillis() + " addServiceSupport transformation completed successfully" )

        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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

        logger.debug(System.currentTimeMillis() + " add CollectNominal transformation completed successfully" )

        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " addSupport transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " addSupport transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " renameColumnVilleExtremiteA transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " dropColumnLibelleRap transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " changeIOSField transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
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
        logger.debug(System.currentTimeMillis() + " join Wasac and Acort transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Add blank columns to given fields.
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

        logger.debug(System.currentTimeMillis() + " addBlankCols1 transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Add blank columns to given fields.
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

        logger.debug(System.currentTimeMillis() + " addBlankCols2 transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Add blank columns to given fields.
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

        logger.debug(System.currentTimeMillis() + " addBlankCols3 transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Select related columns from the dataframe.
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

        logger.debug(System.currentTimeMillis() + " changeIOSField transformation completed successfully" )
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * UDF for Int to String
   */
  val tostring = udf[String, Int](_.toString)

  /**
   * UDF for concat strings
   */
  val getConcatenated = udf((a: String, b: String, c: String, d: String, e: String) =>
    if (a != "" && b != "" && c != "" && d != "" && e != "") (a + ' ' + b + ' ' + c + ' ' + d + '(' + e + ')')
    else null)
}
