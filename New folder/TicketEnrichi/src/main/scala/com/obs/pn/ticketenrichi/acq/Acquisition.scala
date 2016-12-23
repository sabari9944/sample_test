package com.obs.pn.ticketenrichi.acq

import org.apache.spark.sql.DataFrame
import parquet.org.slf4j.LoggerFactory
import org.apache.spark.sql.types.StructType
import com.obs.pn.ticketenrichi.logging.CommonLogger
import com.obs.pn.commons.Utils
import com.obs.pn.ticketenrichi.commons.TicketEnrichiConstants

object Acquisition {

  val conf = Utils.conf
  val sc = Utils.sc
  val sqlContext = Utils.sqlContext
  val prop = Utils.prop
  val logger = LoggerFactory.getLogger(classOf[CommonLogger])
  var rDate: String = Utils.runDate
  /**
   * Load File Function where parserLib is not required
   */
  def loadFile(fileName: String): DataFrame =
    {
      try {
        val res = sqlContext.read
          .format(TicketEnrichiConstants.FORMAT)
          .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
          .option(TicketEnrichiConstants.INFERSCHEMA, TicketEnrichiConstants.TRUE)
          .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE) 
          .option(TicketEnrichiConstants.CHARSET, TicketEnrichiConstants.CHARSETVALUE)
          .load(fileName)

        logger.info("Loadfile completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          sqlContext.emptyDataFrame
        }
      }
    }
  /**
   * Load File Function where parserLib is  required
   */
  def loadFileUnivocity(fileName: String): DataFrame =
    {
      try {
        val res = sqlContext.read
          .format(TicketEnrichiConstants.FORMAT)
          .option(TicketEnrichiConstants.PARSERLIB, TicketEnrichiConstants.UNIVOCITY)
          .option(TicketEnrichiConstants.DELIMITER, TicketEnrichiConstants.SEMICOLON)
          .option(TicketEnrichiConstants.INFERSCHEMA, TicketEnrichiConstants.TRUE) 
          .option(TicketEnrichiConstants.HEADER, TicketEnrichiConstants.TRUE)
          .option(TicketEnrichiConstants.CHARSET, TicketEnrichiConstants.CHARSETVALUE)
          .load(fileName)
        logger.info("loadFile completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          sqlContext.emptyDataFrame
        }
      }

    }

}
