
package com.obs.pn.parcmarine2.transf

import com.obs.pn.acq.Acquisition
import com.obs.pn.parcmarine2.commons.SubGraph1Helper
import com.obs.pn.commons.Utils
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

/**
 * This class joins file  BrEds, BrGar, BrHpr, BrIpr2, BrIsr, BrTie
 */
object SubGraph1 {

  /** logger */
  val logger = LoggerFactory.getLogger(SubGraph1.getClass)

  /**
   * Transformations for SubGraph1
   */
  def execute(): DataFrame = {
    val prop = Utils.prop

    /** Load the BrEds, BrGar, BrHpr, BrIpr2, BrIsr, BrTie files */
    logger.info("loading TIMESTAMP BR_EDS");
    val fileBrEds = Acquisition.loadTimestampFile("dev.BR_EDS", Utils.runDate, false);

    logger.info("loading TIMESTAMP BR_GAR");
    val fileBrGar = Acquisition.loadTimestampFile("dev.BR_GAR", Utils.runDate, false);

    logger.info("loading TIMESTAMP BR_HPR");
    val fileBrHpr = Acquisition.loadTimestampFile("dev.BR_HPR", Utils.runDate, false);

    logger.info("loading TIMESTAMP BR_IPR2");
    var fileBrIpr2 = Acquisition.loadTimestampFile("dev.BR_IPR2", Utils.runDate, false);

    logger.info("loading TIMESTAMP WASAC");
    val fileBrIsr = Acquisition.loadTimestampFile("dev.BR_ISR", Utils.runDate, false);

    logger.info("loading TIMESTAMP BR_TIE");
    val fileBrTie = Acquisition.loadTimestampFile("dev.BR_TIE", Utils.runDate, false);

    /** Performing the joins and select transformation */
    val transBrHpr = SubGraph1Helper.selectFieldsFromBrHpr(fileBrHpr)

    /** Creating new column ipr_idtprd on file BR_IPR2 and fill it with ipr_idtprd */
    logger.info("adding column ipr_idtprd to BR_IPR2")

    fileBrIpr2 = fileBrIpr2.withColumn("ipr_idtprd", Utils.tostring(fileBrIpr2("ipr_idtprd")))
    logger.info("fileBRIpr2 count : " + fileBrIpr2.count().toString())

    /** Calling a transformation function */
    val fileBrTieSelect = SubGraph1Helper.selectFieldsFromBrTie(fileBrTie)
    logger.info("fileBrTieSelect count : " + fileBrTieSelect.count().toString())

    /** join BrIsr and BrEds */
    val join1 = fileBrEds.as('a).join(fileBrIsr.as('b), fileBrEds("eds_idteds") === fileBrIsr("eds_idteds"), "left_outer")
    logger.info("join1 count : " + join1.count().toString())

    /** Calling a transformation function */
    val join1out = SubGraph1Helper.selectFieldsFromBrEdsJoinBrIsr(join1)
    logger.info("join1out count : " + join1out.count().toString())

    /** join BrIpr2 and BrHpr */
    val join2 = fileBrIpr2.as('a).join(transBrHpr.as('b), fileBrIpr2("ipr_idtprd") === transBrHpr("ipr_idtprd"), "inner")
    logger.info("join2 count : " + join2.count().toString())

    /** Calling a transformation function */
    val join2out = SubGraph1Helper.selectFieldsFromBrIpr2joinBrHpr(join2)
    logger.info("join2out count : " + join2out.count().toString())

    /** join output of join2 and BrGar */
    val join3 = join2out.as('a).join(fileBrGar.as('b), join2out("ipr_idtprd") === fileBrGar("ipr_idtprd"), "left_outer")
    logger.info("join3 count : " + join3.count().toString())

    /** Calling a transformation function */
    val join3out = SubGraph1Helper.selectFieldsFromBrGarJoin(join3)
    logger.info("join3out count : " + join3out.count().toString())

    /** join output of join1 and output of join3 */
    val join4 = join1out.as('a).join(join3out.as('b), join1out("isr_idtsoures") === join3out("isr_idtsoures"), "inner")
    logger.info("join4 count : " + join4.count().toString())

    /** Calling a transformation function */
    val join4out = SubGraph1Helper.selectFieldsFromBrEdsBrIsrJoinBrGar(join4)
    logger.info("join4out count : " + join4out.count().toString())

    /** join output of join4 and BrTie */
    val join5 = join4out.as('a).join(fileBrTieSelect.as('b), join4out("tie_idttie") === fileBrTieSelect("tie_idttie"), "inner")
    logger.info("join5 count : " + join5.count().toString())

    /** Select required fields from final join output */
    val join5out = SubGraph1Helper.selectFieldsFromAllJoionsFinalResultset(join5)
    logger.info("join5out count : " + join5out.count().toString())

    /** Add a column to final join output */
    val joinGraph1Out = SubGraph1Helper.addColumnToFinalResultset(join5out)

    /** Saving the sub_graph_1 output file */
    //Acquisition.writeFile(joinGraph1Out, Acquisition.getFilename("dev.output_sub_graph_1_pn_parc_marine2"))

    println("END OF SUBGRAPH 1, counting lines : " + joinGraph1Out.count())

    /** Returning the output DataFrame 1 */
    joinGraph1Out
  }
}