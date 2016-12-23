package com.obs.pn.ticketenrichi.acq.incidentologiewasac

import com.obs.pn.ticketenrichi.acq.Acquisition
import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.ticketenrichi.commons.Utils

object IncidentologieWasac {
  /**
   * Load IncidentologieWasac File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFile(Utils.propFileLoader("dev.wasac"))
    return res
  }

  /**
   * Transforamtion to replace the column containing \n with ""
   */
  def replaceWasac(wasac: DataFrame): DataFrame = {
    val replacedWASAC = wasac.na.replace("ios", Map("\n" -> ""))
    return replacedWASAC
  }

}
