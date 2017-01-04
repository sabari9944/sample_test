
package com.obs.pn.parcmarine2.transf

import com.obs.pn.acq.Acquisition
import com.obs.pn.parcmarine2.commons.Transformations
import com.obs.pn.commons.Utils
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

/**
 * This class loads BrEds, BrGar, BrHpr, BrIpr2, BrIsr, BrTie files
 * and perform transformation on them
 */
object SubGraph1 {

  /** logger */
  val logger = LoggerFactory.getLogger(SubGraph1.getClass)

  /**
   * Transformations for SubGraph1
   * @param verbose boolean for caching / un-caching dataFrames
   * @return DataFrame which will be used in SubGraph3
   */
  def execute(verbose: Boolean): DataFrame = {
    try {

      logger.info("############ Beggining phase 1 ############")
      /** Loading Configuration */
      val prop = Utils.prop

      /** Loading file */
      val fileBrHpr = Acquisition.loadTimestampFile("dev.BR_HPR", Utils.runDate, false);

      /** Performing the joins and select transformation */
      val transBrHpr = Transformations.selectFieldsFromBrHpr(fileBrHpr)

      /** Try to remove DataFrame from Cache */
      Utils.unloadDataFrame(verbose, fileBrHpr)

      /** Creating new column ipr_idtprd on file BR_IPR2 and fill it with ipr_idtprd */
      logger.debug("adding column ipr_idtprd to BR_IPR2")

      /** Loading file */
      var fileBrIpr2 = Acquisition.loadTimestampFile("dev.BR_IPR2", Utils.runDate, false);

      /** Adding one more column */
      fileBrIpr2 = fileBrIpr2.withColumn("ipr_idtprd", Utils.tostring(fileBrIpr2("ipr_idtprd")))

      /** Loading file */
      val fileBrTie = Acquisition.loadTimestampFile("dev.BR_TIE", Utils.runDate, false);

      /** Calling a transformation function */
      val fileBrTieSelect = Transformations.selectFieldsFromBrTie(fileBrTie)

      /** Try to remove DataFrame from Cache */
      Utils.unloadDataFrame(verbose, fileBrTie)

      /** Loading file */
      val fileBrEds = Acquisition.loadTimestampFile("dev.BR_EDS", Utils.runDate, false);

      /** Loading file */
      val fileBrIsr = Acquisition.loadTimestampFile("dev.BR_ISR", Utils.runDate, false);

      /** join BrIsr and BrEds */
      val join1 = fileBrEds.as('a).join(fileBrIsr.as('b), fileBrEds("eds_idteds") === fileBrIsr("eds_idteds"), "left_outer")

      /** Try to remove DataFrame from Cache */
      Utils.unloadDataFrame(verbose, fileBrIsr)
      Utils.unloadDataFrame(verbose, fileBrEds)

      /** Calling a transformation function */
      val join1out = Transformations.selectFieldsFromBrEdsJoinBrIsr(join1)

      /** join BrIpr2 and BrHpr */
      val join2 = fileBrIpr2.as('a).join(transBrHpr.as('b), fileBrIpr2("ipr_idtprd") === transBrHpr("ipr_idtprd"), "inner")

      /** Try to remove DataFrame from Cache */
      Utils.unloadDataFrame(verbose, fileBrIpr2)

      /** Calling a transformation function */
      val join2out = Transformations.selectFieldsFromBrIpr2joinBrHpr(join2)

      /** Loading file */
      val fileBrGar = Acquisition.loadTimestampFile("dev.BR_GAR", Utils.runDate, false);

      /** join output of join2 and BrGar */
      val join3 = join2out.as('a).join(fileBrGar.as('b), join2out("ipr_idtprd") === fileBrGar("ipr_idtprd"), "left_outer")

      /** Try to remove DataFrame from Cache */
      Utils.unloadDataFrame(verbose, fileBrGar)

      /** Calling a transformation function */
      val join3out = Transformations.selectFieldsFromBrGarJoin(join3)

      /** join output of join1 and output of join3 */
      val join4 = join1out.as('a).join(join3out.as('b), join1out("isr_idtsoures") === join3out("isr_idtsoures"), "inner")

      /** Calling a transformation function */
      val join4out = Transformations.selectFieldsFromBrEdsBrIsrJoinBrGar(join4)

      /** join output of join4 and BrTie */
      val join5 = join4out.as('a).join(fileBrTieSelect.as('b), join4out("tie_idttie") === fileBrTieSelect("tie_idttie"), "inner")

      /** Select required fields from final join output */
      val join5out = Transformations.selectFieldsFromAllJoionsFinalResultset(join5)

      /** Add a column to final join output */
      val joinGraph1Out = Transformations.addColumnToFinalResultset(join5out)

      logger.info("############ Ending phase 1 ############")

      /** Returning the output DataFrame 1 */
      joinGraph1Out
    } catch {
      case e: Exception => {
        logger.error(System.currentTimeMillis() + "### Unable to run module 1 out of 3 ###")
        throw e
      }
    }
  }
}