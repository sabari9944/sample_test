
package com.obs.pn

import java.util.Calendar
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.{ Config, ConfigFactory }

object sub_graph_3 
{
  /* Creating the schema for INSEE output */
case class schema(nom_commune: String, poulation_tot: String, code_insee: String, nom_commune_caps: String, code_postal: String, nom_commune_caps2: String)

/* Main */
def main(args: Array[String]) 
 {
val conf = new SparkConf().setAppName("sub_graph_3")
val sc = new SparkContext(conf)
val sqlContext = new HiveContext(sc)
val prop = ConfigFactory.load()

/* INSEE */
/* code_postal fixed length 5 so we are adding 0 */
def correct_code_postal(s:String): String =
  {
    if(s!=null && s.length()==5) s
    else if(s!=null && s.length()==4) "0".concat(s)
    else s
  }  
/* Replace ("-", " ") and ("û", "u") */
def correct_nom_commune(s:String):String =
  {
    val result = s.replace("-", " ").replace("û", "u")
    result
  }
/* Reading all the field from communes_INSEE_utile */
val communes = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.communes_INSEE_utile"))
/* Reading all the field from correspondance-code-insee-code-postal_utile */
val correspondance = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.correspondance-code-insee-code-postal_utile"))
import sqlContext.implicits._ 
/* communes_INSEE_utile Inner join  correspondance-code-insee-code-postal_utile key:code_insee */
val insee = communes.as('a).join(correspondance.as('b), $"a.code_insee" === $"b.Code_INSEE","inner")
/* Selecting  nom_commune, poulation_tot, code_insee, nom_commune_caps from communes_INSEE_utile 
 * and 
 * Code_Postal, Commune from correspondance-code-insee-code-postal_utile   */
val insee_out = insee.select($"a.nom_commune",$"a.poulation_tot",$"a.code_insee",$"a.nom_commune_caps",$"b.Code_Postal",$"a.nom_commune_caps")
/* Applying function for Code_Postal and Commune  */
val insee_out_df= insee_out.map(x => {schema(x(0).toString,x(1).toString,x(2).toString,x(3).toString,correct_code_postal(x(4).toString),correct_nom_commune(x(5).toString))}).toDF
/* INSEE  completed */

/* INSEE  with Allrap */
/* Reading  Incidentologie_wasac join ACORT join AllRapReference  output file*/ 
val allrap_out = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").option("parserLib", "univocity").load(prop.getString("dev.sub_graph_2_output"))
/* Allrap output left outer join INee output   */
val allrap_join_insee = allrap_out.as('a).join(insee_out_df.as('b),allrap_out("cp_extremite_A") === insee_out_df("code_postal") && allrap_out("ville_extremite_A") === insee_out_df("nom_commune_caps2") ,"left_outer").drop(allrap_out("population"))
/* Select all filed's from Allrap output and  poulation_tot from INSEE */ 
val allrap_join_insee_select = allrap_join_insee.select($"a.*",$"b.poulation_tot")
/* Rename poulation_tot to population */
val allrap_join_insee_output = allrap_join_insee_select.withColumnRenamed("poulation_tot", "population") 
/* INSEE  with Allrap completed */

/* Incidentologie IAI extract [ ios and nb_paires ] */
/* Reading incidentologie_wasac_iai   */ 
val df_incidentologie_wasac_iai = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("parserLib", "univocity").option("header", "true").option("inferSchema", "true").load(prop.getString("dev.incidentologie_wasac_iai"))                    
/* Checking for not Null able column on fk_num_test and resultat_test   */
val df_wasac_iai = df_incidentologie_wasac_iai.select("*").withColumn("fk_num_test",when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test",when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))  
/* Apply filter on fk_num_test and resultat_test for IOS flow  */
val df_wasac_iai_ios= df_wasac_iai.filter($"fk_num_test" === 9 && $"resultat_test" === "OK")
/* Rename message to ios_version_from_iai */
val df_wasac_iai_replace= df_wasac_iai_ios.na.replace("message",Map("\n" -> "")).withColumnRenamed("message", "ios_version_from_iai")
/* Roll-up on key:feuillet to find max(date_res) */
val df_wasac_iai_rollup=df_wasac_iai_replace.groupBy("feuillet").agg(max("date_res").alias("date_res"))
/* Rollup out Inner join incidentologie_wasac_iai filter  */
val df_wasac_iai_join= df_wasac_iai_replace.as('a).join(df_wasac_iai_rollup.as('b), $"a.feuillet" === $"b.feuillet" && $"a.date_res"===$"b.date_res","inner")
/* Extracting all the filed from incidentologie_wasac_iai filter and dropping ticketid  */
val df_wasac_join_ios = df_wasac_iai_join.select($"a.*").drop("ticketid")
/* suffix function */
def suffix(s:String): String =
  {
  if(s!=null ) s.takeRight(1) else null  
  }     
/* Checking for not Null able column on fk_num_test and resultat_test   */
val df_wasac_nb = df_incidentologie_wasac_iai.select("*").withColumn("fk_num_test",when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test",when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))
/* Apply filter on fk_num_test and resultat_test for NB Paires flow  */
val df_wasac_filter_nb= df_wasac_nb.filter($"fk_num_test"===60 && $"resultat_test" === "OK")  
/* Apply suffix to message and rename message to nb_paires */
val df_wasac_suffix=df_wasac_filter_nb.withColumn("message", when($"message" !== "", $"message".toString().takeRight(1)).otherwise($"message")).withColumnRenamed("message", "nb_paires")
/* Roll-up on key:feuillet to find max(date_res) */
val df_wasac_rollup_nb_paires = df_wasac_suffix.groupBy("feuillet").agg(max("date_res").alias("date_res"))
/* Rollup out Inner join incidentologie_wasac_iai filter  */
val df_wasac_join_nb= df_wasac_rollup_nb_paires.as('a).join(df_wasac_suffix.as('b), $"a.feuillet" === $"b.feuillet" && $"a.date_res"===$"b.date_res","inner")
/* Select all the filed from incidentologie_wasac_iai filter and drop ticketid */
val df_wasac_join_nb_paires = df_wasac_join_nb.select($"b.*").drop("ticketid")
/* Ios and nb_paires compleded */

/* Ios join started */
/*  AllRap, Insee out left outer join incidentologie_wasac_iai (ios)  */
val ios_join = allrap_join_insee_output.as('a).join(df_wasac_join_ios.as('b), allrap_join_insee_output("identifiant_1_produit") === df_wasac_join_ios("feuillet"),"left_outer").drop(allrap_join_insee_output("ios_version_from_iai"))
/* Select all the field from AllRap, Insee out and ios_version_from_iai from incidentologie_wasac_iai (ios) */ 
val ios_join_output = ios_join.select($"a.*",$"b.ios_version_from_iai") 
/*ios join compledted */

/* nb_paires join started */
/*AllRap, Insee out and incidentologie_wasac_iai (ios) left outer join  incidentologie_wasac_iai (nb_paires)  */
val nb_paires_join = ios_join_output.as('a).join(df_wasac_join_nb_paires.as('b), ios_join_output("identifiant_1_produit") === df_wasac_join_nb_paires("feuillet"),"left_outer").drop(ios_join_output("nb_paires"))
/* Select all the field from AllRap, Insee, ios_version_from_iai and nb_paires from incidentologie_wasac_iai (nb_paires) */ 
val nb_paires_join_out = nb_paires_join.select($"a.*",$"b.nb_paires")
/* nb_paires join compledted */

/* final join */
/* Reading the output of  (BR_EDS and BR_ISR ) join (IPR and BR_GAR) join (BR_TIE)  */
val sub_graph_1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", prop.getString("dev.separator")).option("inferSchema","true").option("header","true").load(prop.getString("dev.output_sub_graph_1_pn_parc_marine2"))
/*  (BR_EDS, BR_ISR, IPR, BR_GAR, BR_TIE)  Left outer join ( Wasac, Acort, AllRap, Insee, ios_version_from_iai ) */
val final_join = sub_graph_1.as('a).join(nb_paires_join_out.as('b),sub_graph_1("ipr_idtcom") === nb_paires_join_out("identifiant_1_produit"),"left_outer")
/* Select the filed according to the target table PN_PARC_MARINE */
val final_join_output = final_join.select($"a.ipr_idtcom",$"a.eds_idteds",$"a.ipr_idtetaprd",$"b.rap",$"b.libelle_rap",$"a.ipr_lbtypprd",$"a.ipr_datmescom",$"b.ce_id",$"b.connexion_id",$"b.support_id",$"b.service_support",$"b.collect",$"b.support",$"b.nb_paires",$"b.collect_role",$"b.router_role",$"a.gar_lbcodgar",$"a.gar_lbplghor",$"a.tie_raiscltie",$"a.tie_addresstie",$"a.tie_codptlcomtie",$"a.tie_sirtie",$"a.ipr_steutlexta",$"a.ipr_comexta",$"a.ipr_voibpexta",$"a.ipr_codptlcomexta",$"a.ipr_steutlextb",$"a.ipr_comextb",$"a.ipr_voibpextb",$"a.ipr_codptlcomextb",$"b.population",$"b.population_cat",$"b.constructeur",$"b.chassis",$"b.ios_version",$"b.ios_version_from_iai",$"b.ip_admin",$"b.rsc",$"b.version_boot",$"b.dslam_id",$"b.master_dslam_id",$"b.nortel_id",$"b.pe_id",$"b.fav_id",$"b.ntu_id",$"b.tronc_type",$"b.num_pivot",$"b.element_reseau",$"b.type_interne")
/* Rename the filed according to the target table PN_PARC_MARINE */
val final_join_output_rename = final_join_output.withColumnRenamed("ipr_idtcom", "identifiant_1_produit").withColumnRenamed("eds_idteds", "identifiant_eds_pilote").withColumnRenamed("ipr_idtetaprd", "etat_produit").withColumnRenamed("ipr_lbtypprd","type_produit").withColumnRenamed("tie_raiscltie", "raison_sociale").withColumnRenamed("tie_sirtie", "siren").withColumnRenamed("ipr_voibpexta", "voie_extremite_A").withColumnRenamed("ipr_codptlcomexta", "cp_extremite_A").withColumnRenamed("ipr_comexta", "ville_extremite_A").withColumnRenamed("ipr_steutlexta", "societe_extremite_A").withColumnRenamed("ipr_steutlextb", "societe_extremite_B").withColumnRenamed("ipr_comextb", "ville_extremite_B").withColumnRenamed("ipr_voibpextb", "voie_extremite_B").withColumnRenamed("ipr_codptlcomextb", "cp_extremite_B").withColumnRenamed("gar_lbcodgar", "gtr").withColumnRenamed("gar_lbplghor", "plage_horaire_gtr").withColumnRenamed("tie_addresstie", "addresse_complete_client").withColumnRenamed("tie_codptlcomtie", "code_postal_client").withColumn("date_photo", current_date())
/*Sort the output based on identifiant_1_produit */
val final_join_output_sort = final_join_output_rename.orderBy("identifiant_1_produit")
/* Remove the duplicate */
/* Selecting the partition key */
val Key = Window.partitionBy($"identifiant_1_produit")
/* Group by identifiant_1_produit and assign row number  */ 
val PN_PARC_MARINE_remove_duplicate = final_join_output_sort.select($"*", rowNumber.over(Key).alias("rn"))

/* Group by identifiant_1_produit and order by row number in desc  */ 
val Key_1 = Window.partitionBy($"identifiant_1_produit").orderBy(desc("rn"))
/* Select the first column of Row number and drop row number field */
val PN_PARC_MARINE_remove_duplicate_1 = PN_PARC_MARINE_remove_duplicate.select($"*", rowNumber.over(Key_1).alias("rn_1")).where($"rn_1" === 1).drop("rn").drop("rn_1")
val PN_PARC_MARINE = PN_PARC_MARINE_remove_duplicate_1

/* Save the output in Hive location */
PN_PARC_MARINE.write.mode(SaveMode.Overwrite).parquet(prop.getString("dev.output_sub_graph_3_pn_parc_marine2"))
sc.stop()
} 
}

