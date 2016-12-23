package com.obs.pn.ticketenrichi.acq.allrapreference

import com.obs.pn.ticketenrichi.acq.Acquisition
import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.commons.Utils
import Utils.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object AllRapReference {
  /**
   * Load AllRapRefrence File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFileUnivocity(prop.getString("dev.rapReference"))
    return res
  }

}
