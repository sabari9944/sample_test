package com.obs.pn.ticketenrichi.acq.incidentologiewasaciai

import com.obs.pn.ticketenrichi.acq.Acquisition
import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import Utils.sqlContext.implicits._
object IncidentologieWasacIai {
  /**
   * Load IncidentologieWasacIai File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFileUnivocity(Utils.propFileLoader("dev.wasac_iai"))
    return res
  }
  /**
   * Transformation on WasacIai for Ios
   */
  def transformIos(wasacIai: DataFrame): DataFrame = {
    val wasacIosFilter = filterWasacIos(wasacIai)
    val replaceRenameWasacCol = replaceRenameColumnIos(wasacIosFilter)
    val wasacIosRollup = wasacRollupIos(replaceRenameWasacCol)
    val wasacIaiIosJoin = wasacIosJoin(replaceRenameWasacCol, wasacIosRollup)
    return wasacIaiIosJoin
  }
  /**
   * Transformation on WasacIai for NbPaires
   */
  def transformNbPaires(wasacIai: DataFrame): DataFrame = {
    val wasacIaiNbPairesFilter = wasacNbPairesFilter(wasacIai)
    val nbPairesCol = suffixWasacIai(wasacIaiNbPairesFilter)
    val rollupIaiNbPaires = rollupNbPaires(nbPairesCol)
    val wasacIaiNbPaires = wasacNbPairesJoin(nbPairesCol, rollupIaiNbPaires)
    return wasacIaiNbPaires
  }

  /**
   * Filter the record to match the condition.
   *
   */
  def filterWasacIos(wasacIai: DataFrame): DataFrame = {

    val wasacSelect = wasacIai.select("*").withColumn("fk_num_test", when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test", when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))
    val wasacFilter = wasacSelect.filter($"fk_num_test" === 9 && $"resultat_test" === "OK")
    return wasacFilter
  }

  /**
   * Perform the function to replace column value and Rename the column
   */

  def replaceRenameColumnIos(wasacIai: DataFrame): DataFrame = {

    val replaceRename = wasacIai.withColumn("message",regexp_replace($"message","[\\r\\n]","")).withColumnRenamed("message", "ios_version")
    return replaceRename
  }

  /**
   * Rollup the data on two column with the max value of the third column
   */

  def wasacRollupIos(wasacIai: DataFrame): DataFrame = {
    val wasacIosRollup = wasacIai.groupBy("ticketid", "feuillet").agg(max("date_res").alias("date_res"))
    return wasacIosRollup
  }

  /**
   * Join the renamed data with rollup data to get wasacIaiIos DataFrame
   */

  def wasacIosJoin(wasacRename: DataFrame, wasacRollup: DataFrame): DataFrame = {
    val wasacJoin = wasacRename.as('a).join(wasacRollup.as('b), $"a.ticketid" === $"b.ticketid" && $"a.feuillet" === $"b.feuillet" && $"a.date_res" === $"b.date_res", "inner")
    val wasacIosJoin = wasacJoin.select($"a.*")

    return wasacIosJoin

  }

  /**
   * Filter the record to match the condition.
   */

  def wasacNbPairesFilter(wasacIai: DataFrame): DataFrame = {
    val wasacSelect = wasacIai.select("*").withColumn("fk_num_test", when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test", when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))
    val wasacFilterNbPaires = wasacSelect.filter($"fk_num_test" === 60 && $"resultat_test" === "OK")
    return wasacFilterNbPaires
  }
/**
 * case class to perform string suffix transformation
 */
 
 /**
  * Udf to perform string suffix on column message 
  */

    val suffixTransf = udf((s: String) => if(s!=null && s.takeRight(1).matches("\\d")) s.takeRight(1) else null)
    
  /**
   * Perform suffix on the column message and rename column to nb_paires
   */
  def suffixWasacIai(wasacIai: DataFrame): DataFrame = {
     val removeLine = wasacIai.withColumn("message",regexp_replace($"message","[\\r\\n]",""))
     val suffixMsg= removeLine.withColumn("message",suffixTransf($"message")).withColumnRenamed("message", "nb_paires") 
  return suffixMsg
  }

  /**
   * Rollup the data on two column with the max value of the third column
   */
  def rollupNbPaires(wasacIai: DataFrame): DataFrame = {
    val rollupNbPaires = wasacIai.groupBy("ticketid", "feuillet").agg(max("date_res").alias("date_res"))
    return rollupNbPaires
  }

  /**
   * Join the original data with rollup data for nb_Paires
   */

  def wasacNbPairesJoin(nbPairesCol: DataFrame, rollupNbPaires: DataFrame): DataFrame = {
    val wasacIaiNbPaires = rollupNbPaires.as('a).join(nbPairesCol.as('b), $"a.ticketid" === $"b.ticketid" && $"a.feuillet" === $"b.feuillet" && $"a.date_res" === $"b.date_res", "inner")
    val wasacIaiNbPairesJoin = wasacIaiNbPaires.select($"b.*")
    return wasacIaiNbPairesJoin

  }
  /**
   * Rename column from WasacIaiIos  "ios_version" to "ios_version_from_iai"
   */

  def renameColumnWasacIaiIos(dataFrame: DataFrame): DataFrame = {
    val renamedColWasacIaiIos = dataFrame.withColumnRenamed("ios_version", "ios_version_from_iai")
    return renamedColWasacIaiIos
  }
}
