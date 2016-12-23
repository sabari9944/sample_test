package com.obs.pn.ticketenrichi.acq

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import Utils.sqlContext.implicits._
object Acort {
  /**
   * Load Acort File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFile(Utils.propFileLoader("dev.acort"))
    return res
  }
  /**
   * Intermediate Fosav-wasac-Acort File
   */
  def loadInterimFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFile(prop.getString("dev.filterResult"))
    return res
  }

}
