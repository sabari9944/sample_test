package com.obs.pn.parcmarine2.commons

import parquet.org.slf4j.LoggerFactory
import com.obs.pn.parcmarine2.logging.CommonLogger
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import com.obs.pn.commons.Utils


/**
 * Helper class for SubGraph1. Transformation for SubGraph1 are declared in this Object.
 * Transformation file which does occur in the subgraph1.scala
 */
object SubGraph1Helper {

  /** Logger instance */
  val logger = LoggerFactory.getLogger(SubGraph1Helper.getClass)

  /** Get SQL context */
  val sqlContext = Utils.sqlContext

  /** Get configurations */
  val prop = Utils.prop

  import sqlContext.implicits._
  
  /**
   * selectExpression for BRHPR.
   * 
   * Fields from fileBrHpr are selected with changed field names.
   *
   * @param fileBrHpr dataframe
   */
  def selectFieldsFromBrHpr(fileBrHpr: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrHpr started")

        val res = fileBrHpr.selectExpr("cast(ipr_datdebimgprd as date) as ipr_datdebimgprd",
          "cast(ipr_datfinimgprd as date) as ipr_datfinimgprd", "isr_idtsoures", "ipr_idtetaprd",
          "ipr_idtcom", "cast(ipr_idtprd as string) as ipr_idtprd", "ipr_lbtypprd")
          .withColumn("ipr_lbtypprd", when($"ipr_lbtypprd" === "", null)
            .otherwise($"ipr_lbtypprd")).withColumn("ipr_idtetaprd",
            when($"ipr_idtetaprd" === "", null).otherwise($"ipr_idtetaprd"))

        logger.info("selectFieldsFromBrHpr completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error("Unable to select expression in file BR_HPR", e)
          throw e
        }
      }
    }

  /**
   * selectExpression for BRTIE.
   * 
   * Selecting only required fields from fileBrTie and skipping unwanted fields.
   *
   * @param fileBrHpr dataframe
   * 
   */
  def selectFieldsFromBrTie(fileBrTie: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrTie started")
        
        val res = fileBrTie.selectExpr("tie_idttie", "tie_raiscltie",
          "tie_sirtie", "tie_voibptie", "tie_cmpvoitie", "tie_codptlcomtie", "tie_comtie", "tie_lbpaytie")

        logger.info("selectFieldsFromBrTie completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error("Unable to select expression in file BR_TIE", e)
          throw e
        }
      }
    }

  /**
   * Filtering out required fields from join (fileBrEds and fileBrIsr) output.
   * 
   * @param
   * 
   */
  def selectFieldsFromBrEdsJoinBrIsr(join1: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrEdsJoinBrIsr started")
        val res = join1.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"b.isr_idtsoures", $"b.isr_datdebimgsoures", $"b.isr_datfinimgsoures", $"b.tie_idttie")
          .withColumn("isr_idtsoures", when($"b.isr_idtsoures" === "", null)
            .otherwise($"b.isr_idtsoures"))
          .withColumn("tie_idttie", when($"b.tie_idttie" === "", null)
            .otherwise($"b.tie_idttie"))
        logger.info("selectFieldsFromBrEdsJoinBrIsr completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }

  /**
   * Filtering out required fields from join (BrIpr2 and BrHpr) output.
   */
  def selectFieldsFromBrIpr2joinBrHpr(join2: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrIpr2joinBrHpr started ")
        val res = join2.select($"b.ipr_datdebimgprd", $"b.ipr_datfinimgprd", $"b.isr_idtsoures",
          $"b.ipr_idtetaprd", $"b.ipr_idtcom", $"b.ipr_idtprd", $"b.ipr_lbtypprd", $"a.ipr_voibpexta",
          $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta",
          $"a.ipr_voibpextb", $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb",
          $"a.ipr_steutlextb", $"a.ipr_datmescom")
          .filter($"ipr_idtetaprd" === "PARC PARTIEL" || $"ipr_idtetaprd" === "PARC TOTAL"
            || $"ipr_idtetaprd" === "CARNET" || $"ipr_idtetaprd" === "EN COURS")
          .filter($"ipr_lbtypprd" !== "")

        logger.info("selectFieldsFromBrIpr2joinBrHpr completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }

  /**
   * Filtering out required fields from join (join2 and BrGar) output.
   */
  def selectFieldsFromBrGarJoin(join3: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrGarJoin started ")
        val res = join3.select($"a.ipr_datdebimgprd", $"a.ipr_datfinimgprd", $"a.isr_idtsoures",
          $"a.ipr_idtetaprd", $"a.ipr_idtcom", $"a.ipr_idtprd", $"a.ipr_lbtypprd", $"a.ipr_voibpexta",
          $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta", $"a.ipr_voibpextb",
          $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb", $"a.ipr_steutlextb", $"a.ipr_datmescom",
          $"b.gar_lbcodgar", $"b.gar_lbplghor")

        logger.info("selectFieldsFromBrGarJoin completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }

  /**
   * Filtering out required fields from join (output of join1 and output of join3) .
   */
  def selectFieldsFromBrEdsBrIsrJoinBrGar(join4: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromBrEdsBrIsrJoinBrGar started ")
        val res = join4.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"a.isr_idtsoures",
          $"a.isr_datdebimgsoures", $"a.isr_datfinimgsoures", $"a.tie_idttie", $"b.ipr_datdebimgprd",
          $"b.ipr_datfinimgprd", $"b.ipr_idtetaprd", $"b.ipr_idtcom", $"b.ipr_lbtypprd", $"b.ipr_voibpexta",
          $"b.ipr_comexta", $"b.ipr_codptlcomexta", $"b.ipr_lbpayexta", $"b.ipr_steutlexta", $"b.ipr_voibpextb",
          $"b.ipr_comextb", $"b.ipr_codptlcomextb", $"b.ipr_lbpayextb", $"b.ipr_steutlextb", $"b.ipr_datmescom",
          $"b.gar_lbcodgar", $"b.gar_lbplghor")

        logger.info("selectFieldsFromBrEdsBrIsrJoinBrGar completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }

  /**
   * Filtering out required fields from join (output of join4 and BrTie) output.
   */
  def selectFieldsFromAllJoionsFinalResultset(join5: DataFrame): DataFrame =
    {
      try {
        logger.debug("selectFieldsFromAllJoionsFinalResultset started")
        val res = join5.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"a.isr_idtsoures",
          $"a.isr_datdebimgsoures", $"a.isr_datfinimgsoures", $"a.tie_idttie", $"a.ipr_datdebimgprd",
          $"a.ipr_datfinimgprd", $"a.ipr_idtetaprd", $"a.ipr_idtcom", $"a.ipr_lbtypprd", $"a.ipr_voibpexta",
          $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta", $"a.ipr_voibpextb",
          $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb", $"a.ipr_steutlextb", $"b.tie_raiscltie",
          $"b.tie_sirtie", $"b.tie_codptlcomtie", $"a.ipr_datmescom", $"a.gar_lbcodgar", $"a.gar_lbplghor",
          $"b.tie_voibptie", $"b.tie_cmpvoitie", $"b.tie_comtie", $"b.tie_lbpaytie")

        logger.info("selectFieldsFromAllJoionsFinalResultset completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }

  /**
   * Addind new column tie_addresstie.
   */
  def addColumnToFinalResultset(join5out: DataFrame): DataFrame =
    {
      try {
        logger.debug("addColumnToFinalResultset started")
        val res = join5out.withColumn("tie_addresstie", Utils.getConcatenated($"tie_voibptie",
          $"tie_cmpvoitie", $"tie_codptlcomtie", $"tie_comtie", $"tie_lbpaytie"))
          .select("eds_idteds", "eds_lbapteds", "eds_nomcrteds", "isr_idtsoures", "isr_datdebimgsoures",
            "isr_datfinimgsoures", "tie_idttie", "ipr_datdebimgprd", "ipr_datfinimgprd", "ipr_idtetaprd",
            "ipr_idtcom", "ipr_lbtypprd", "ipr_voibpexta", "ipr_comexta", "ipr_codptlcomexta",
            "ipr_lbpayexta", "ipr_steutlexta", "ipr_voibpextb", "ipr_comextb", "ipr_codptlcomextb",
            "ipr_lbpayextb", "ipr_steutlextb", "tie_raiscltie", "tie_sirtie", "tie_addresstie",
            "tie_codptlcomtie", "ipr_datmescom", "gar_lbcodgar", "gar_lbplghor")
        logger.info("addColumnToFinalResultset completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }

    }
}