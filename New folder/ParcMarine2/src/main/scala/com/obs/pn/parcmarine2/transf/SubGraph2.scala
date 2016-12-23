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
import com.obs.pn.acq.Acquisition

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
  def execute(): DataFrame = {

    /** Load properties */
    val prop = Utils.prop

    /** Load Acort file */
    logger.info("loading TIMESTAMP ACORT");
    val dfAcort = Acquisition.loadTimestampFile("dev.acort", Utils.runDate, false);

    /** Sorted acort file generated, which is used for join */
    var sortedAcort = dfAcort.sort(dfAcort("element_reseau"))

    /** Load Wasac file */
    val fileWasac = Acquisition.loadTimestampFile("dev.wasac", Utils.runDate, false);
    var wasac = Utils.changeIOSField(fileWasac)
    logger.info("wasac count : " + wasac.count().toString())

    /** Join Wasac file with Acort file */
    var joinTransformation = Utils.joinWasacAndAcort(wasac, sortedAcort)
    logger.info("joinTransformation count : " + joinTransformation.count().toString())

    /** 'select' part of Wasac and Acort join after filter */
    var select = joinTransformation.filter(
      joinTransformation("type_interne").startsWith("SU_"))

    logger.info("select count : " + select.count().toString())

    /** 'de-select' part of Wasac and Acort join after filter */
    var deselect = joinTransformation.filter(
      !joinTransformation("type_interne").startsWith("SU_"))
    logger.info("deselect count 0 : " + deselect.count().toString())

    /** Drop field type_interne */
    select = select.drop("num_pivot").drop("element_reseau").drop("type_interne")
    logger.info("select count : " + select.count().toString())

    /** Filter out only required fields from 'select' part */
    select = select
      .withColumn("num_pivot", Utils.createColumnWithQuote(lit("": String)))
      .withColumn("element_reseau", Utils.createColumnWithQuote(lit("": String)))
      .withColumn("type_interne", Utils.createColumnWithQuote(lit("": String)))
    logger.info("select count : " + select.count().toString())

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
    var deselectJoin = deselect.join(lkpTopoTronc, deselect("num_pivot") === lkpTopoTronc("TRONC_num_pivot"), "left_outer")
    logger.info("deselect count 01 : " + deselect.count().toString())

    /** Join deselected part (Wasac and Acort join) with SISU lookup file */
    deselectJoin = deselectJoin.join(lkpSiSu, deselectJoin("num_pivot") === lkpSiSu("SISU_num_pivot"), "left_outer")
    logger.info("deselect count 02 : " + deselect.count().toString())

    /** Join deselected part (Wasac and Acort join) with TypeSi lookup file */
    deselectJoin = deselectJoin.join(lkpTypeSi, deselectJoin("num_pivot") === lkpTypeSi("TYPE_SI_num_pivot"), "left_outer")
    logger.info("deselect count 03 : " + deselect.count().toString())

    /** Group by on acort file records to aggregate duplicate records */
    var acortTypInternArr = dfAcort.groupBy(dfAcort("num_pivot") as "accort_num_pivot")
      .agg(expr("collect_list(type_interne) AS type_interne_array"),
        expr("collect_list(element_reseau) AS element_reseau_array"),
        expr("collect_list(type_si) AS type_si_array"))

    /** changing type from array to string and adding 2 new columns */
    acortTypInternArr = acortTypInternArr.withColumn("rap_tmp_1", Utils.getStringToArray(acortTypInternArr("type_si_array")))
    acortTypInternArr = acortTypInternArr.withColumn("rap_tmp_2", Utils.getStringToArray(acortTypInternArr("type_interne_array")))

    /** Joining the acortTypInternArr and the acortTypInternArr */
    deselectJoin = deselectJoin.join(acortTypInternArr, deselectJoin("num_pivot") === acortTypInternArr("accort_num_pivot"),
      "left_outer")
    logger.info("deselect count 04: " + deselect.count().toString())

    /** Add field rap */
    deselectJoin = deselectJoin.withColumn("rap", Utils.calculateRap(deselectJoin("rap_tmp_1"),
      deselectJoin("rap_tmp_2")))
    logger.info("deselect count 05 : " + deselect.count().toString())

    /** Add field ce_id */
    deselectJoin = deselectJoin.withColumn("ce_id", Utils.calculateCeId(deselectJoin("type_interne_array"),
      deselectJoin("element_reseau_array")))
    logger.info("deselect count 06: " + deselectJoin.count().toString())

    /** Add field connexion_id */
    deselectJoin = deselectJoin.withColumn("connexion_id",
      Utils.calculateConnexionId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 07: " + deselectJoin.count().toString())

    /** Add field support_id */
    deselectJoin = deselectJoin.withColumn("support_id",
      Utils.calculateSupportId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 08: " + deselectJoin.count().toString())

    /** Add field router_role */
    deselectJoin = deselectJoin.withColumn("router_role",
      Utils.calculateRouterRole(deselectJoin("type_interne"), deselectJoin("role")))
    logger.info("deselect count 09: " + deselectJoin.count().toString())

    /** Add blank column */
    deselectJoin = Utils.addBlankCols1(deselectJoin)
    logger.info("deselect count 10: " + deselectJoin.count().toString())

    /** Add field dslam_id */
    deselectJoin = deselectJoin.withColumn("dslam_id",
      Utils.calculateDslamId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 11: " + deselectJoin.count().toString())

    /** Add field master_dslam_id */
    deselectJoin = deselectJoin.withColumn("master_dslam_id", Utils.createNewBlankColumn(deselectJoin("type_interne")))
    logger.info("deselect count 12: " + deselectJoin.count().toString())

    /** Add field nortel_id */
    deselectJoin = deselectJoin.withColumn("nortel_id",
      Utils.calculateNortelId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 13 : " + deselect.count().toString())

    /** Add field pe_id */
    deselectJoin = deselectJoin.withColumn("pe_id",
      Utils.calculatePeId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 14 : " + deselectJoin.count().toString())

    /** Add field fav_id */
    deselectJoin = deselectJoin.withColumn("fav_id",
      Utils.calculateFavId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 15 : " + deselectJoin.count().toString())

    /** Add field ntu_id */
    deselectJoin = deselectJoin.withColumn("ntu_id",
      Utils.calculateNtuId(deselectJoin("type_interne_array"), deselectJoin("element_reseau_array")))
    logger.info("deselect count 15 : " + deselectJoin.count().toString())

    /** Add field tronc_type */
    deselectJoin = deselectJoin.withColumn("tronc_type",
      Utils.calculateTroncType(deselectJoin("type_interne_array")))
    logger.info("deselect count 16 : " + deselectJoin.count().toString())

    deselectJoin = Utils.addBlankCols2(deselectJoin)
    logger.info("deselect count 17 : " + deselectJoin.count().toString())

    /** Rename fields */
    deselectJoin = deselectJoin.withColumnRenamed("feuillet", "identifiant_1_produit")
      .withColumnRenamed("ios", "ios_version")
    logger.info("deselect count 18 : " + deselectJoin.count().toString())

    select = select.withColumnRenamed("feuillet", "identifiant_1_produit")
      .withColumnRenamed("ios", "ios_version")
    logger.info("select count : " + select.count().toString())

    select = Utils.addBlankCols3(select)
    logger.info("select count : " + select.count().toString())

    /** Select only required columns and skip rest columns */
    deselectJoin = Utils.selectOutputColumns(deselectJoin)
    logger.info("deselect count 19 : " + deselect.count().toString())

    /** Load AllRapReference file */
    val allRapReference = Acquisition.loadFile("dev.rapReference", false);
    logger.info("allRapReference count : " + allRapReference.count().toString())

    /** Drop unwanted field */
    val gatherInput = Utils.dropColumnLibelleRap(deselectJoin)
    logger.info("deselect count 20 : " + deselectJoin.count().toString())

    /** Join deselect output and AllRapReference file */
    var transformGather = Utils.joinGatherAndAllRapRef(gatherInput, allRapReference)
    logger.info("transformGather count 1 : " + transformGather.count().toString())

    /** Add field ServiceSupport */
    transformGather = Utils.addServiceSupport(transformGather)
    logger.info("transformGather count 2 : " + transformGather.count().toString())

    /** Add field CollectNominal */
    transformGather = Utils.addCollectNominal(transformGather)
    logger.info("transformGather count 3 : " + transformGather.count().toString())

    /** Add field Support */
    transformGather = Utils.addSupport(transformGather)
    logger.info("transformGather count 4: " + transformGather.count().toString())

    /** Replace the strings ST with Saint */
    transformGather = Utils.replaceStString(transformGather)
    logger.info("transformGather count 5: " + transformGather.count().toString())

    /** Rename the column */
    transformGather = Utils.renameColumnVilleExtremiteA(transformGather)
    logger.info("transformGather count 6: " + transformGather.count().toString())

    /** Write the output of subgraph2 to disk */
    //Acquisition.writeFile(transformGather, Acquisition.getFilename("dev.output_sub_graph_2_pn_parc_marine2"))
    logger.info("END OF SUBGRAPH 2, counting lines : " + transformGather.count().toString())

    // NOTE : Task to be done; make a union between transformgather and select dataframe
    val subGraph2Output = transformGather.unionAll(select)
    
    subGraph2Output
  }
}