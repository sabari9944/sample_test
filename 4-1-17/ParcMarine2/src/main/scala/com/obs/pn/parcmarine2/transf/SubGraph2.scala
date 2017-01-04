package com.obs.pn.parcmarine2.transf
import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import com.obs.pn.commons.Utils
import com.obs.pn.parcmarine2.commons.Transformations
import com.obs.pn.acq.Acquisition
import org.apache.spark.sql.functions.broadcast

/**
 * This class joins files  Acort, Wasac and AllRapReference.
 * Generates lookup files from acort files.
 * The lookup files are joined with output of (Acort, Wasac and AllRapReference) join.
 * Here lookup logic is implemented using inner join.
 */
object SubGraph2 {

  /** logger */
  val logger = LoggerFactory.getLogger(SubGraph2.getClass)

  /**
   * All the joins and transformations for SubGraph2
   */
  def execute(verbose: Boolean): DataFrame = {

    logger.info("############ Beggining phase 2 ############")
    /** Load properties */
    val prop = Utils.prop

    /** Load Acort file */
    logger.info("loading TIMESTAMP ACORT");
    val dfAcort = Acquisition.loadTimestampFile("dev.acort", Utils.runDate, false);
    Utils.keepDataFrame(dfAcort)

    /** Sorted acort file generated, which is used for join */
    val sortedAcort = dfAcort.sort(dfAcort("element_reseau"))

    /** Load Wasac file */
    val fileWasac = Acquisition.loadTimestampFile("dev.incidentologie_wasac", Utils.runDate, false);
    val wasac = Transformations.changeIOSField(fileWasac)
    
    /** Try to remove DataFrame from Cache */
    Utils.unloadDataFrame(verbose, fileWasac)

    /** Join Wasac file with Acort file */
    val joinTransformation = Transformations.joinWasacAndAcort(wasac, sortedAcort)

    /** 'select' part of Wasac and Acort join after filter */
    var select = joinTransformation.filter(
      joinTransformation("type_interne").startsWith("SU_"))

    /** 'de-select' part of Wasac and Acort join after filter */
    var deselect = joinTransformation.filter(
      !joinTransformation("type_interne").startsWith("SU_"))

    /** Drop field type_interne */
    select = select.drop("num_pivot").drop("element_reseau").drop("type_interne")

    /** Filter out only required fields from 'select' part */
    select = select
      .withColumn("num_pivot", Utils.createColumnWithQuote(lit("": String)))
      .withColumn("element_reseau", Utils.createColumnWithQuote(lit("": String)))
      .withColumn("type_interne", Utils.createColumnWithQuote(lit("": String)))

    /** Generate TopoTronc lookup file and will have less column in order to optimize joins */
    var lkpTopoTronc = (dfAcort.filter(dfAcort("type_interne").
      startsWith("TOPO_TRONC")))

    /** Generate SiSu lookup file */
    var lkpSiSu = (dfAcort.filter(dfAcort("type_si") === "S-SU"))

    /** Generate TypeSi lookup file */
    var lkpTypeSi = (dfAcort.filter(dfAcort("type_si").startsWith("A-Z")))

    /** Select required fields from TopoTronc lookup file */
    lkpTopoTronc = lkpTopoTronc.select(
      lkpTopoTronc("num_pivot") as ("TRONC_num_pivot"),
      lkpTopoTronc("element_reseau") as ("TRONC_element_reseau"),
      lkpTopoTronc("type_interne") as ("TRONC_type_interne"))

    /** Select required fields from SiSu lookup file */
    lkpSiSu = lkpSiSu.select(
      lkpSiSu("num_pivot") as ("SISU_num_pivot"),
      lkpSiSu("element_reseau") as ("SISU_element_reseau"),
      lkpSiSu("type_interne") as ("SISU_type_interne"))

    /** Select required fields from TypeSi lookup file */
    lkpTypeSi = lkpTypeSi.select(
      lkpTypeSi("num_pivot") as ("TYPE_SI_num_pivot"),
      lkpTypeSi("element_reseau") as ("TYPE_SI_element_reseau"),
      lkpTypeSi("type_interne") as ("TYPE_SI_type_interne"))

    /** Join deselected part (Wasac and Acort join) with TopoTronc lookup file */
    var deselectJoin = deselect.join(broadcast(lkpTopoTronc), deselect("num_pivot") === lkpTopoTronc("TRONC_num_pivot"), "left_outer")

    /** Join deselected part (Wasac and Acort join) with SISU lookup file */
    deselectJoin = deselectJoin.join(broadcast(lkpSiSu), deselectJoin("num_pivot") === lkpSiSu("SISU_num_pivot"), "left_outer")

    /** Join deselected part (Wasac and Acort join) with TypeSi lookup file */
    deselectJoin = deselectJoin.join(broadcast(lkpTypeSi), deselectJoin("num_pivot") === lkpTypeSi("TYPE_SI_num_pivot"), "left_outer")

    /** Group by on acort file records to aggregate duplicate records */
    var acortTypInternArr = dfAcort.groupBy(dfAcort("num_pivot") as "accort_num_pivot")
      .agg(expr("collect_list(type_interne) AS type_interne_array"),
        expr("collect_list(element_reseau) AS element_reseau_array"),
        expr("collect_list(type_si) AS type_si_array"))

    /** Try to remove DataFrame from Cache */
    Utils.unloadDataFrame(verbose, dfAcort)

    /** changing type from array to string and adding 2 new columns */
    acortTypInternArr = acortTypInternArr.withColumn("rap_tmp_1", Utils.getStringToArray(acortTypInternArr("type_si_array")))
    acortTypInternArr = acortTypInternArr.withColumn("rap_tmp_2", Utils.getStringToArray(acortTypInternArr("type_interne_array")))

    /** Joining the acortTypInternArr and the acortTypInternArr */
    deselectJoin = deselectJoin.join(acortTypInternArr, deselectJoin("num_pivot") === acortTypInternArr("accort_num_pivot"),
      "left_outer")

    /** Add field rap */
    deselectJoin = deselectJoin.withColumn("rap", Utils.calculateRap(deselectJoin("rap_tmp_1"),
      deselectJoin("rap_tmp_2")))

    /** Add field ce_id */
    deselectJoin = deselectJoin.withColumn("ce_id", Utils.calculateCeId(deselectJoin("type_interne_array"),
      deselectJoin("element_reseau_array")))

    /** Add field connexion_id */
    deselectJoin = deselectJoin.withColumn("connexion_id",
      Utils.calculateConnexionId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field support_id */
    deselectJoin = deselectJoin.withColumn("support_id",
      Utils.calculateSupportId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field router_role */
    deselectJoin = deselectJoin.withColumn("router_role",
      Utils.calculateRouterRole(deselectJoin("type_interne"), deselectJoin("role")))

    /** Add blank column */
    deselectJoin = Transformations.addBlankCols1(deselectJoin)

    /** Add field dslam_id */
    deselectJoin = deselectJoin.withColumn("dslam_id",
      Utils.calculateDslamId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field master_dslam_id */
    deselectJoin = deselectJoin.withColumn("master_dslam_id", Utils.createNewBlankColumn(deselectJoin("type_interne")))

    /** Add field nortel_id */
    deselectJoin = deselectJoin.withColumn("nortel_id",
      Utils.calculateNortelId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field pe_id */
    deselectJoin = deselectJoin.withColumn("pe_id",
      Utils.calculatePeId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field fav_id */
    deselectJoin = deselectJoin.withColumn("fav_id",
      Utils.calculateFavId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field ntu_id */
    deselectJoin = deselectJoin.withColumn("ntu_id",
      Utils.calculateNtuId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))

    /** Add field tronc_type */
    deselectJoin = deselectJoin.withColumn("tronc_type",
      Utils.calculateTroncType(deselectJoin("type_interne_array")))

    deselectJoin = Transformations.addBlankCols2(deselectJoin)

    /** Rename fields */
    deselectJoin = deselectJoin.withColumnRenamed("feuillet", "identifiant_1_produit")
      .withColumnRenamed("ios", "ios_version")

    select = select.withColumnRenamed("feuillet", "identifiant_1_produit")
      .withColumnRenamed("ios", "ios_version")

    select = Utils.addBlankCols3(select)

    /** Select only required columns and skip rest columns */
    deselectJoin = Utils.selectOutputColumns(deselectJoin)

    /** Select only required columns and skip rest columns */
    select = Utils.selectOutputColumns(select)

    /** Union of select and de-select part */
    val unionSelectDeselect = deselectJoin.unionAll(select)

    /** Load AllRapReference file */
    val allRapReference = Acquisition.loadFile("dev.AllRapReference", false);

    /** Drop unwanted field */
    val gatherInput = Transformations.dropColumnLibelleRap(unionSelectDeselect)

    /** Join deselect output and AllRapReference file */
    var transformGather = Transformations.joinGatherAndAllRapRef(gatherInput, allRapReference)

    /** Try to remove DataFrame from Cache */
    Utils.unloadDataFrame(verbose, dfAcort)

    /** Add field ServiceSupport */
    transformGather = Transformations.addServiceSupport(transformGather)

    /** Add field CollectNominal */
    transformGather = Transformations.addCollectNominal(transformGather)

    /** Add field Support */
    transformGather = Transformations.addSupport(transformGather)

    /** Replace the strings ST with Saint */
    transformGather = Utils.replaceStString(transformGather)

    /** Rename the column */
    transformGather = Transformations.renameColumnVilleExtremiteA(transformGather)

    logger.info("############ Ending phase 2 ############")

    transformGather
  }
}