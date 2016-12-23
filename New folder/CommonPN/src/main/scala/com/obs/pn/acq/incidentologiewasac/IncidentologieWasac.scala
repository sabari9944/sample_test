package com.obs.pn.acq.incidentologiewasac

import com.obs.pn.acq.Acquisition
import org.apache.spark.sql.DataFrame
import com.obs.pn.acq.Acquisition
import com.obs.pn.commons.Utils

object IncidentologieWasac {
  
  /**
   * Transforamtion to replace the column containing \n with ""
   */
  def replaceWasac(wasac: DataFrame): DataFrame = {
    val replacedWASAC = wasac.na.replace("ios", Map("\n" -> ""))
    return replacedWASAC
  }

}
