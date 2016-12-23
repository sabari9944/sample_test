package com.obs.pn.ticketenrichi.acq.nrgtrreferentiel

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.commons.Utils

object NRGTRReferentiel {
  /**
   * Load NRGTRReferentiel File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFileUnivocity(prop.getString("dev.NRGTRreferentiel"))
    return res
  }
}