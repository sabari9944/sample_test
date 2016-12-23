package com.obs.pn.ticketenrichi.acq.incidentologiefosav

import com.obs.pn.ticketenrichi.acq.Acquisition
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.rdd.RDD

object IncidentologieFosav {
  /**
   * Load File IncidentologieFosav
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFile(Utils.propFileLoader("dev.fosav"))
    return res
  }
  /**
   * Load interim file after Fosav and Wasac join
   */
  def loadInterim(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFile(prop.getString("dev.intermediateFosavWasac"))
    return res
  }
  /**
   * Call method for transformation
   */
  def transform(fosav: DataFrame): DataFrame = {
    val repStr = replaceString(fosav)
    val rddDF = rddToDF(fosav, repStr)
    val transRes = renameColumns(rddDF)
    return transRes
  }

  /**
   * Transformation to replace '\x92' with '
   */
  def replaceString(fosav: DataFrame): RDD[Row] = {
    val replacedFOSAV = fosav.map(row => {
      val x = row.getAs[String](24)
      val detail_pbm = if (x.contains("\\x92")) x.replace("\\x92", "'") else x
      Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9),
        row(10), row(11), row(12), row(13), row(14), row(15), row(16), row(17), row(18), row(19),
        row(20), row(21), row(22), row(23), detail_pbm, row(25), row(26), row(27), row(28), row(29),
        row(30), row(31), row(32), row(33), row(34), row(35), row(36), row(37), row(38), row(39),
        row(40), row(41), row(42), row(43), row(44), row(45), row(46), row(47), row(48), row(49),
        row(50), row(51), row(52), row(53), row(54), row(55), row(56), row(57), row(58), row(59))
    })
    return replacedFOSAV
  }

  /**
   * Reconvert the rdd formed in above steps to Dataframe
   */

  def rddToDF(fosav: DataFrame, replacedDf: RDD[Row]): DataFrame = {

    val dfFOSAV = fosav.sqlContext.createDataFrame(replacedDf, fosav.schema)
    return dfFOSAV
  }

  /**
   * Rename the column name "pht_idtcom" to "feuillet"
   */
  def renameColumns(fosav: DataFrame): DataFrame = {
    val renamedDf = fosav.withColumnRenamed("pht_idtcom", "feuillet")
    return renamedDf
  }

}
