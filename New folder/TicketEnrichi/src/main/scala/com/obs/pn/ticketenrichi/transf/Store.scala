package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import com.obs.pn.commons.Utils
import com.obs.pn.ticketenrichi.commons.TicketEnrichiConstants
import org.apache.spark.sql.SaveMode

object Store {
  val prop = Utils.prop

  def storeFosavWasacFilter(fosavWasacFilter: DataFrame) {
    fosavWasacFilter.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.intermediateFosavWasac"))
  }

  def storeFilter(filter: DataFrame) = {
    filter.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.filterResult"))
  }

  def storeInsee(insee: DataFrame) {
    insee.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.inseeResult"))
  }

  def storeHive(ticketEnrich: DataFrame) = {

    ticketEnrich.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.hive_pn_ticket_enriche"))

    ticketEnrich.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.intermediate_pn_ticket_enriche"))
  }

  def interimMediateStore(interimLoad: DataFrame) = {
    interimLoad.write
      .format(TicketEnrichiConstants.FORMAT)
      .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
      .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
      .mode(SaveMode.Overwrite)
      .save(prop.getString("dev.intermediate_operational"))
  }

}
