package com.obs.pn.ticketenrichi.acq.lkpactivations

import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import com.obs.pn.commons.Utils.sqlContext.implicits._

object LKPActivations {
  val logger: org.slf4j.Logger = LoggerFactory.getLogger(LKPActivations.getClass)
  /**
   * Returns a DataFrame on which lookup Next logic is implemented on Activations File
   * This method returns DataFrame with 5 column for the same key field extracted from the File. 
   * 
   * @param dataframe The DataFrame which is obtained after reading Activation File on which looku_Next is implemented
   * @return comb4 Datrame with key Field and the 5 column which is required for further transformation in TicketEnrichis
   */

  def transform(dataFrame: DataFrame): DataFrame = {
    logger.debug("transform")
    val windowPartition = Window.partitionBy($"pht_idttic")
    val lkpActivations = dataFrame.select($"*", rowNumber.over(windowPartition).alias("rn"))

    val rn1 = lkpActivations.filter($"rn" === 1).withColumnRenamed("act_idtedsdes", "eds1").select($"pht_idttic", $"eds1")
    val rn2 = lkpActivations.filter($"rn" === 2).withColumnRenamed("act_idtedsdes", "eds2").withColumnRenamed("pht_idttic", "phtidttic1").select($"phtidttic1", $"eds2")
    val rn3 = lkpActivations.filter($"rn" === 3).withColumnRenamed("act_idtedsdes", "eds3").withColumnRenamed("pht_idttic", "phtidttic2").select($"phtidttic2", $"eds3")
    val rn4 = lkpActivations.filter($"rn" === 4).withColumnRenamed("act_idtedsdes", "eds4").withColumnRenamed("pht_idttic", "phtidttic3").select($"phtidttic3", $"eds4")
    val rn5 = lkpActivations.filter($"rn" === 5).withColumnRenamed("act_idtedsdes", "eds5").withColumnRenamed("pht_idttic", "phtidttic4").select($"phtidttic4", $"eds5")

    val comb1 = rn1.as('a).join(rn2.as('b), $"a.pht_idttic" === $"b.phtidttic1", "left_outer")

    val comb2 = comb1.as('a).join(rn3.as('b), $"a.pht_idttic" === $"b.phtidttic2", "left_outer")

    val comb3 = comb2.as('a).join(rn4.as('b), $"a.pht_idttic" === $"b.phtidttic3", "left_outer")

    val comb4 = comb3.as('a).join(rn5.as('b), $"a.pht_idttic" === $"b.phtidttic4", "left_outer")

    comb4.select("pht_idttic", "eds1", "eds2", "eds3", "eds4", "eds5")

  }

}
