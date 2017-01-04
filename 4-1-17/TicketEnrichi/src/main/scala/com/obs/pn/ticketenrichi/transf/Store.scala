package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import com.obs.pn.commons.Utils
import com.obs.pn.acq.Acquisition
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

object Store {
  val logger: org.slf4j.Logger = LoggerFactory.getLogger(Store.getClass)
  /**
   * Writes to the File for Hive Storage and Creation of History File
   * @param ticketEnrich DataFrame which is to be written
   */

  def storeHive(ticketEnrich: DataFrame)  = {
      logger.debug("storeHive")
      Acquisition.writeFile(ticketEnrich,Acquisition.getFilename("dev.hive_pn_ticket_enriche"),false)
      
      Acquisition.writeFile(ticketEnrich,Acquisition.getFilename("dev.intermediate_pn_ticket_enriche"),true) 
  }
   /**
   * Writes to the File For creation of LookUp Repetitif
   * @param interimLoad DataFrame which is to be written
   */

  def interimMediateStore(interimLoad: DataFrame) = {
    logger.debug("interimMediateStore")
    Acquisition.writeFile(interimLoad,Acquisition.getFilename("dev.intermediate_operational"),true)
  }
  
}
