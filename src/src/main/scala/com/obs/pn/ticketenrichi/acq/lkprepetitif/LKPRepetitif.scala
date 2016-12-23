package com.obs.pn.ticketenrichi.acq.lkprepetitif

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Utils.sqlContext.implicits._
import java.text.SimpleDateFormat
import java.sql.Timestamp
import com.obs.pn.ticketenrichi.transf.Store

object LKPRepetitif {
  /**
   * Load Lookup LKPRepetitif File
   */
  val prop = Utils.prop
  def loadFile(): DataFrame = {

    val res = Acquisition.loadFileUnivocity(prop.getString("dev.lkp_repetitifs"))
    return res
  }
  /**
   * Load Record from previous day
   */
  def interMediateloadFile(): DataFrame = {

    val res = Acquisition.loadFileUnivocity(prop.getString("dev.intermediate_pn_ticket_enriche"))
    return res
  }
  /**
   * Load File For Look Replicate
   */
  def operationalLoadFile(): DataFrame = {

    val res = Acquisition.loadFile(prop.getString("dev.intermediate_operational"))
    return res
  }

  /**
   * Transformation using lookup file , to fill "oui" or "non"
   */
  def transform(dataFrame: DataFrame): DataFrame = {
    val isRepetitif = dataFrame.withColumn("is_repetitif", when(($"nb_repetitions".isNotNull) || ($"nb_repetitions" !== "") || ($"nb_repetitions" !== "null"), "oui").otherwise("non")).drop("nb_repetitions")
    return isRepetitif
  }

  def lookReplicate(): DataFrame = {
    val interimLoad = interMediateloadFile()
    Store.interimMediateStore(interimLoad)
    val operationalLoad = operationalLoadFile()
    val optLoadCol = operationalLoad.withColumn("date_debut_ticket", $"date_debut_ticket".cast("string"))
    val optLoadColMonth = optLoadCol.withColumn("date_debut_ticket_result", add_months(Utils.getTimestamp(($"date_debut_ticket")), -1))
    val optLoadFilter = optLoadColMonth.filter($"is_gtrisable" === "Oui")
      .filter(($"ce_id".isNotNull) || ($"ce_id" !== "") ||  ($"ce_id" !== null) ||  ($"ce_id" !== "null"))
      .filter((Utils.getTimestamp($"date_debut_ticket")).gt($"date_debut_ticket_result"))
    val optLoadDist = optLoadFilter.select("num_ticket", "ce_id", "category", "subcategory").distinct
    val optLoadGroup = optLoadDist.select("ce_id", "category", "subcategory").groupBy("ce_id", "category", "subcategory").count().withColumnRenamed("count","nb_repetitions")
    val lkpRep = optLoadGroup.filter($"nb_repetitions".gt("1"))
    return lkpRep
  }

}
