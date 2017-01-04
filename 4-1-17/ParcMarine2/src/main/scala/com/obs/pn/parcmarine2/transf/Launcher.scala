package com.obs.pn.parcmarine2.transf

import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory
import com.obs.pn.commons.Utils

/**
 * This is the entry point for Marine2 flow execution. Marine2 flow has been divided into three parts
 * which will run one after another.
 */
object Launcher {
  /** logger for the launcher class */
  val logger = LoggerFactory.getLogger(Launcher.getClass)

  def main(args: Array[String]): Unit = {
    /** Checking arguments */
    if (args.length < 2) {
      /** System validation for date parameter, this is system err not println */
      System.err.println("Launch Parameter Required:  <Date:yyyymmdd> <HDFSPATH: Config File> ")
        /**
         * Zero => Everything Okay
         * Positive => Something I expected could potentially go wrong went wrong (bad command-line, can't find file, could not connect to server)
         * Negative => Something I didn't expect at all went wrong (system error - unanticipated exception - externally forced termination e.g. kill -9)
         */
      System.exit(1)
    }

    try {
      /** Setting new properties */
      System.setProperty("obspn.application.conf", args(1));
      System.setProperty("obspn.application.master", "");
      System.setProperty("obspn.application.appname", "parcmarine2");
      logger.info(" RUN DATE " + args(0));
      /** Overwriting the Date */
      Utils.runDate = args(0)

      /** Starting ParcMarine2 */
      parcmarine2()
    } catch {
      case e: Exception => {
        //Slf4J does NOT have the FATAL level
        logger.error("FATAL ERROR while running Parc Marine 2 process", e)
        /**
         * Zero => Everything Okay
         * Positive => Something I expected could potentially go wrong went wrong (bad command-line, can't find file, could not connect to server)
         * Negative => Something I didn't expect at all went wrong (system error - unanticipated exception - externally forced termination e.g. kill -9)
         */
        System.exit(-1)
      }

    }

  }

  /**
   * Fonction that runs all the ParcMarine transformations
   * Stores in ram each temporary DataFrame if verbose = true
   */
  def parcmarine2() : Unit = {
    /** Logger instance */
    val logger = LoggerFactory.getLogger(Launcher.getClass)

    /** Verbose variable for unloading dataFrame */
    val verbose = true

    /** Run transformations from SubGraph1 */
    val dataFrame1 = SubGraph1.execute(verbose)

    /** Run transformations from SubGraph2 */
    val dataFrame2 = SubGraph2.execute(verbose)

    /** Run transformations from SubGraph3 */
    SubGraph3.execute(dataFrame1, dataFrame2)
  }
}
