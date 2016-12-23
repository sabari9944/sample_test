package com.obs.pn.ticketenrichi.acq.categoriesincidents

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.sql.functions.lit
import Utils.sqlContext.implicits._

object CategoriesIncidents {
  /**
   * Load CategoriesIncidents File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFileUnivocity(prop.getString("dev.CategoriesIncident"))
    return res
  }

}
