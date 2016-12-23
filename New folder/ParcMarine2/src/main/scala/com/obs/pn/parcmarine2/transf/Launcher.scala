package com.obs.pn.parcmarine2.transf

import org.slf4j.LoggerFactory
import com.obs.pn.parcmarine2.logging.CommonLogger
import com.obs.pn.commons.Utils

/**
 * This is the entry point for Marine2 flow execution. Marine2 flow has been divided into three parts
 * which will run one after another.
 */
object Launcher {

  /** logger */
  val logger = LoggerFactory.getLogger(Launcher.getClass)

  def main(args: Array[String]): Unit = {

    /**
     *  Read data parameter from command line
     */

    if (args.length < 2) {
      /**
       * System validation for date parameter, this is system err not println
       */
      System.err.println("Launch Parameter Required:  <Date:yyyymmdd> <HDFSPATH: Config File> ")
      System.exit(1)
    } else {

      System.setProperty("obspn.application.conf", args(1));
      System.setProperty("obspn.application.master", "");
      System.setProperty("obspn.application.appname", "parcmarine2");

      logger.info(" RUN DATE " + args(0));
      Utils.runDate = args(0)
    }

    parcmarine2()
  }

  def parcmarine2() = {

    /** Logger instance*/
    val logger = LoggerFactory.getLogger(Launcher.getClass)

    try {
      /** Run transformations from SubGraph1 */
      logger.info(System.currentTimeMillis() + "Starting SubGraph1")
      val dataFrame1 = SubGraph1.execute()
      logger.info(System.currentTimeMillis() + " SubGraph1 completed successfully")

      /** Run transformations from SubGraph2 */
      logger.info(System.currentTimeMillis() + "Starting SubGraph2")
      val dataFrame2 = SubGraph2.execute()
      logger.info(System.currentTimeMillis() + " SubGraph2 completed successfully")

      /** Run transformations from SubGraph3 */
      logger.info(System.currentTimeMillis() + "Starting SubGraph3")
      SubGraph3.execute(dataFrame1, dataFrame2)
      logger.info(System.currentTimeMillis() + " SubGraph3 completed successfully")

    } catch {
      /** Log the exception and stop the execution of flow*/
      case e: Exception => {
        logger.error(e.getMessage, e)
        throw e
      }
    }

  }
}