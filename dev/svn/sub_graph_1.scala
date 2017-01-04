
package com.obs.pn

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
import org.apache.spark.rdd.RDD
import com.typesafe.config.{ Config, ConfigFactory }


object sub_graph_1 
{
def main(args: Array[String]) 
{
val conf = new SparkConf().setAppName("sub_graph_1") 
val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val prop = ConfigFactory.load()
import sqlContext.implicits._

// Read the file BR_EDS
val BR_EDS = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_EDS"))
// Read the file BR_GAR
val BR_GAR = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_GAR"))
// Read the file BR_HPR
val BR_HPR = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_HPR"))
val BR_HPR_1 = BR_HPR.selectExpr("cast(ipr_datdebimgprd as date) as ipr_datdebimgprd" , "cast(ipr_datfinimgprd as date) as ipr_datfinimgprd", "isr_idtsoures", "ipr_idtetaprd", "ipr_idtcom", "cast(ipr_idtprd as string) as ipr_idtprd","ipr_lbtypprd" ).withColumn("ipr_lbtypprd", when($"ipr_lbtypprd" === "", "null").otherwise($"ipr_lbtypprd")).withColumn("ipr_idtetaprd", when($"ipr_idtetaprd" === "", "null").otherwise($"ipr_idtetaprd"))
// Read the file BR_IPR2
val BR_IPR2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_IPR2"))
/* TO String */
val tostring = udf[String, Int]( _.toString)
val BR_IPR2_1 = BR_IPR2.withColumn("ipr_idtprd", tostring(BR_IPR2("ipr_idtprd")))
// Read the file BR_ISR
val BR_ISR = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_ISR"))
// Read the file BR_TIE
val BR_TIE = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.BR_TIE"))
val BR_TIE_1 = BR_TIE.selectExpr("tie_idttie","tie_raiscltie","tie_sirtie","tie_voibptie","tie_cmpvoitie","tie_codptlcomtie","tie_comtie","tie_lbpaytie")

/* BR_EDS left outer join BR_ISR */
val join_1 = BR_EDS.as('a).join(BR_ISR.as('b), BR_EDS("eds_idteds") === BR_ISR("eds_idteds"),"left_outer")
/* Select the required column from BR_EDS and BR_ISR */
val join_1_out = join_1.select($"a.eds_idteds",$"a.eds_lbapteds",$"a.eds_nomcrteds",$"b.isr_idtsoures",$"b.isr_datdebimgsoures",$"b.isr_datfinimgsoures",$"b.tie_idttie").withColumn("isr_idtsoures", when($"b.isr_idtsoures" === "", "null").otherwise($"b.isr_idtsoures")).withColumn("tie_idttie", when($"b.tie_idttie" === "", "null").otherwise($"b.tie_idttie"))
/* BR_EDS left outer join BR_ISR completed */

/* IPR [ BR_IPR2 and BR_HPR ] join start */
val join_2 = BR_IPR2_1.as('a).join(BR_HPR_1.as('b),BR_IPR2_1("ipr_idtprd") === BR_HPR_1("ipr_idtprd"),"inner")
val join_2_out = join_2.select($"b.ipr_datdebimgprd",$"b.ipr_datfinimgprd",$"b.isr_idtsoures",$"b.ipr_idtetaprd",$"b.ipr_idtcom",$"b.ipr_idtprd",$"b.ipr_lbtypprd",$"a.ipr_voibpexta",$"a.ipr_comexta",$"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta", $"a.ipr_voibpextb",$"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb", $"a.ipr_steutlextb", $"a.ipr_datmescom" ).filter($"ipr_idtetaprd" === "PARC PARTIEL" || $"ipr_idtetaprd" === "PARC TOTAL" || $"ipr_idtetaprd" === "CARNET" || $"ipr_idtetaprd" === "EN COURS").filter($"ipr_lbtypprd" !== "" )
/* IPR [ BR_IPR2 and BR_HPR ] join completed */

/* IPR and BR_GAR join start */
val join_3 = join_2_out.as('a).join(BR_GAR.as('b),join_2_out("ipr_idtprd") === BR_GAR("ipr_idtprd"),"left_outer")
val join_3_out = join_3.select($"a.ipr_datdebimgprd",$"a.ipr_datfinimgprd",$"a.isr_idtsoures",$"a.ipr_idtetaprd",$"a.ipr_idtcom",$"a.ipr_idtprd",$"a.ipr_lbtypprd",$"a.ipr_voibpexta",$"a.ipr_comexta",$"a.ipr_codptlcomexta",$"a.ipr_lbpayexta",$"a.ipr_steutlexta",$"a.ipr_voibpextb",$"a.ipr_comextb",$"a.ipr_codptlcomextb",$"a.ipr_lbpayextb",$"a.ipr_steutlextb",$"a.ipr_datmescom",$"b.gar_lbcodgar",$"b.gar_lbplghor")
/* IPR and BR_GAR join completed */

/* (BR_EDS and BR_ISR ) join (IPR and BR_GAR) start */
val join_4 = join_1_out.as('a).join(join_3_out.as('b),join_1_out("isr_idtsoures") === join_3_out("isr_idtsoures"),"inner")
val join_4_out = join_4.select($"a.eds_idteds",$"a.eds_lbapteds",$"a.eds_nomcrteds" ,$"a.isr_idtsoures",$"a.isr_datdebimgsoures",$"a.isr_datfinimgsoures",$"a.tie_idttie" ,$"b.ipr_datdebimgprd",$"b.ipr_datfinimgprd",$"b.ipr_idtetaprd",$"b.ipr_idtcom",$"b.ipr_lbtypprd",$"b.ipr_voibpexta",$"b.ipr_comexta",$"b.ipr_codptlcomexta",$"b.ipr_lbpayexta",$"b.ipr_steutlexta",$"b.ipr_voibpextb",$"b.ipr_comextb",$"b.ipr_codptlcomextb",$"b.ipr_lbpayextb",$"b.ipr_steutlextb",$"b.ipr_datmescom",$"b.gar_lbcodgar",$"b.gar_lbplghor")
/* (BR_EDS and BR_ISR ) join (IPR and BR_GAR) completed */

/* (BR_EDS and BR_ISR ) join (IPR and BR_GAR) join (BR_TIE) start */
val join_5 = join_4_out.as('a).join(BR_TIE_1.as('b),join_4_out("tie_idttie") === BR_TIE_1("tie_idttie"),"inner")
val join_5_out =join_5.select($"a.eds_idteds",$"a.eds_lbapteds",$"a.eds_nomcrteds",$"a.isr_idtsoures",$"a.isr_datdebimgsoures",$"a.isr_datfinimgsoures",$"a.tie_idttie",$"a.ipr_datdebimgprd",$"a.ipr_datfinimgprd",$"a.ipr_idtetaprd",$"a.ipr_idtcom",$"a.ipr_lbtypprd",$"a.ipr_voibpexta",$"a.ipr_comexta",$"a.ipr_codptlcomexta",$"a.ipr_lbpayexta",$"a.ipr_steutlexta",$"a.ipr_voibpextb",$"a.ipr_comextb",$"a.ipr_codptlcomextb",$"a.ipr_lbpayextb",$"a.ipr_steutlextb",$"b.tie_raiscltie",$"b.tie_sirtie",$"b.tie_codptlcomtie",$"a.ipr_datmescom",$"a.gar_lbcodgar",$"a.gar_lbplghor",$"b.tie_voibptie",$"b.tie_cmpvoitie",$"b.tie_comtie",$"b.tie_lbpaytie")

/* Concatenated for tie_addresstie */
val getConcatenated = udf((a: String, b: String, c: String, d: String, e: String) => if ( a != "" && b != "" && c != "" && d != "" && e != "" )( a + ' ' + b + ' ' + c + ' ' + d + '(' + e + ')' )else null)
val join_5_out_1 = join_5_out.withColumn("tie_addresstie",getConcatenated($"tie_voibptie",$"tie_cmpvoitie",$"tie_codptlcomtie",$"tie_comtie",$"tie_lbpaytie")).select("eds_idteds","eds_lbapteds","eds_nomcrteds","isr_idtsoures","isr_datdebimgsoures","isr_datfinimgsoures","tie_idttie","ipr_datdebimgprd","ipr_datfinimgprd","ipr_idtetaprd","ipr_idtcom","ipr_lbtypprd","ipr_voibpexta","ipr_comexta","ipr_codptlcomexta","ipr_lbpayexta","ipr_steutlexta","ipr_voibpextb","ipr_comextb","ipr_codptlcomextb","ipr_lbpayextb","ipr_steutlextb","tie_raiscltie","tie_sirtie","tie_addresstie","tie_codptlcomtie","ipr_datmescom","gar_lbcodgar","gar_lbplghor")
val sub_graph_1_output = join_5_out_1
sub_graph_1_output.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", prop.getString("dev.separator")).mode(SaveMode.Overwrite).save(prop.getString("dev.output_sub_graph_1_pn_parc_marine2"))
/* (BR_EDS and BR_ISR ) join (IPR and BR_GAR) join (BR_TIE) completed */

sc.stop()
}
}