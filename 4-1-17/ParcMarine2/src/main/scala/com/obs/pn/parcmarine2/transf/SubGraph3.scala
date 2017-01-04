package com.obs.pn.parcmarine2.transf

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

import com.obs.pn.commons.Utils
import com.obs.pn.acq.Acquisition
import org.slf4j.LoggerFactory

/**
 *  This class takes the output of SubGraph1 and SubGraph2.
 *  Applies join transformation from files communes_INSEE_utile, correspondance-code-insee-code-postal_utile.
 *  Writes the final output for Marine2 table on the disk
 */
object SubGraph3 {
  val logger = LoggerFactory.getLogger(SubGraph3.getClass)

  logger.info("############ Beggining phase 3 ############")
  /** Creating the schema for INSEE output */
  case class schema(nom_commune: String, poulation_tot: String, code_insee: String, nom_commune_caps: String, code_postal: String, nom_commune_caps2: String)

  /** Transformations for SubGraph3 */
  def execute(dataframe1: DataFrame, dataFrame2: DataFrame): Unit = {

    val sqlContext = Utils.sqlContext
    val prop = Utils.prop

    /** code_postal fixed length 5 so we are adding 0 */
    def correctingCodePostal(s: String): String =
      {
        if (s != null && s.length() == 5) s
        else if (s != null && s.length() == 4) "0".concat(s)
        else s
      }
    /** Replace ("-", " ") and ("û", "u") */
    def correctingNomCommune(s: String): String =
      {
        val result = s.replace("-", " ").replace("û", "u")
        result
      }

    /** Reading all the field from communes_INSEE_utile */
    val communes = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", Acquisition.SEMICOLON).option("inferSchema", "true").option("header", "true").load(Acquisition.getFilename("dev.communes_INSEE_utile"))
    logger.info("DELIMITER LOADED")
    /** Reading all the field from correspondance-code-insee-code-postal_utile */
    val correspondance = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", Acquisition.SEMICOLON).option("inferSchema", "true").option("header", "true").load(Acquisition.getFilename("dev.correspondance-code-insee-code-postal_utile"))

    /** Note check the import location */
    import sqlContext.implicits._

    /** communes_INSEE_utile Inner join  correspondance-code-insee-code-postal_utile key:code_insee */
    val insee = communes.as('a).join(correspondance.as('b), $"a.code_insee" === $"b.Code_INSEE", "inner")
    /**
     * Selecting  nom_commune, poulation_tot, code_insee, nom_commune_caps from communes_INSEE_utile
     * and
     * Code_Postal, Commune from correspondance-code-insee-code-postal_utile
     */
    val inseeOut = insee.select($"a.nom_commune", $"a.poulation_tot", $"a.code_insee", $"a.nom_commune_caps", $"b.Code_Postal", $"a.nom_commune_caps")

    /** Applying function for Code_Postal and Commune  */
    val inseeOutDf = inseeOut.map(x => { schema(x(0).toString, x(1).toString, x(2).toString, x(3).toString, correctingCodePostal(x(4).toString), correctingNomCommune(x(5).toString)) }).toDF
    /** INSEE  completed */

    /** Join INSEE  with Allrap */
    /** Reading  Incidentologie_wasac join ACORT join AllRapReference  output file*/
    val allrapOut = dataFrame2

    /** Allrap output left outer join INee output   */
    val allrapJoinInsee = allrapOut.as('a).join(inseeOutDf.as('b), allrapOut("cp_extremite_A") === inseeOutDf("code_postal") && allrapOut("ville_extremite_A") === inseeOutDf("nom_commune_caps2"), "left_outer").drop(allrapOut("population"))

    /** Select all filed's from Allrap output and  poulation_tot from INSEE */
    val allrapJoinInseeSelect = allrapJoinInsee.select($"a.*", $"b.poulation_tot")

    /** Rename poulation_tot to population */
    val allrapJoinInseeOutput = allrapJoinInseeSelect.withColumnRenamed("poulation_tot", "population")

    /** Extract fields [ ios and nb_paires ] from Incidentologie IAI  */
    /** Reading incidentologie_wasac_iai   */
    val dfIncidentologieWasacIai = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", Acquisition.SEMICOLON).option("parserLib", "univocity").option("header", "true").option("inferSchema", "true").load(Acquisition.getTimestampFilename("dev.incidentologie_wasac_iai", Utils.runDate))

    /** Checking for not Null able column on fk_num_test and resultat_test   */
    val dfWasacIai = dfIncidentologieWasacIai.select("*").withColumn("fk_num_test", when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test", when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))

    /** Apply filter on fk_num_test and resultat_test for IOS flow  */
    val dfWasacIaiIos = dfWasacIai.filter($"fk_num_test" === 9 && $"resultat_test" === "OK")

    /** Rename message to ios_version_from_iai */
    val dfWasacIaiReplace = dfWasacIaiIos.na.replace("message", Map("\n" -> "")).withColumnRenamed("message", "ios_version_from_iai")

    /** Roll-up on key:feuillet to find max(date_res) */
    val dfWasacIaiRollup = dfWasacIaiReplace.groupBy("feuillet").agg(max("date_res").alias("date_res"))

    /** Rollup out Inner join incidentologie_wasac_iai filter  */
    val dfWasacIaiJoin = dfWasacIaiReplace.as('a).join(dfWasacIaiRollup.as('b), $"a.feuillet" === $"b.feuillet" && $"a.date_res" === $"b.date_res", "inner")

    /** Extracting all the filed from incidentologie_wasac_iai filter and dropping ticketid  */
    val dfWasacJoinIos = dfWasacIaiJoin.select($"a.*").drop("ticketid")

    /** suffix function */
    def suffix(s: String): String =
      {
        if (s != null) s.takeRight(1) else null
      }

    /** Checking for not Null able column on fk_num_test and resultat_test   */
    val dfWasacNb = dfIncidentologieWasacIai.select("*").withColumn("fk_num_test", when($"fk_num_test".isNull or $"fk_num_test" === "", 0).otherwise($"fk_num_test")).withColumn("resultat_test", when($"resultat_test".isNull or $"resultat_test" === "", "null").otherwise($"resultat_test"))

    /** Apply filter on fk_num_test and resultat_test for NB Paires flow  */
    val dfWasacFilterNb = dfWasacNb.filter($"fk_num_test" === 60 && $"resultat_test" === "OK")

    /** Apply suffix to message and rename message to nb_paires */
    val dfWasacSuffix = dfWasacFilterNb.withColumn("message", when($"message" !== "", $"message".toString().takeRight(1)).otherwise($"message")).withColumnRenamed("message", "nb_paires")

    /** Roll-up on key:feuillet to find max(date_res) */
    val dfWasacRollupNbPaires = dfWasacSuffix.groupBy("feuillet").agg(max("date_res").alias("date_res"))

    /** Rollup out Inner join incidentologie_wasac_iai filter  */
    val dfWasacJoinNb = dfWasacRollupNbPaires.as('a).join(dfWasacSuffix.as('b), $"a.feuillet" === $"b.feuillet" && $"a.date_res" === $"b.date_res", "inner")

    /** Select all the filed from incidentologie_wasac_iai filter and drop ticketid */
    val dfWasacJoinNbPaires = dfWasacJoinNb.select($"b.*").drop("ticketid")

    /** join Ios and allrapref, Insee */
    /**  AllRap, Insee out left outer join incidentologie_wasac_iai (ios)  */
    val iosJoin = allrapJoinInseeOutput.as('a).join(dfWasacJoinIos.as('b), allrapJoinInseeOutput("identifiant_1_produit") === dfWasacJoinIos("feuillet"), "left_outer").drop(allrapJoinInseeOutput("ios_version_from_iai"))

    /** Select all the field from AllRap, Insee out and ios_version_from_iai from incidentologie_wasac_iai (ios) */
    val iosJoinOutput = iosJoin.select($"a.*", $"b.ios_version_from_iai")

    /** AllRap, Insee out and incidentologie_wasac_iai (ios) left outer join  incidentologie_wasac_iai (nb_paires)  */
    val nbPairesJoin = iosJoinOutput.as('a).join(dfWasacJoinNbPaires.as('b), iosJoinOutput("identifiant_1_produit") === dfWasacJoinNbPaires("feuillet"), "left_outer").drop(iosJoinOutput("nb_paires"))

    /** Select all the field from AllRap, Insee, ios_version_from_iai and nb_paires from incidentologie_wasac_iai (nb_paires) */
    val nbPairesJoinOut = nbPairesJoin.select($"a.*", $"b.nb_paires")

    /** nb_paires join completed */
    /** Reading the output of  (BR_EDS and BR_ISR ) join (IPR and BR_GAR) join (BR_TIE)  */
    val subGraph1 = dataframe1 //sqlContext.read.format("com.databricks.spark.csv").option("delimiter", Acquisition.SEMICOLON).option("inferSchema", "true").option("header", "true").load(Acquisition.getFilename("dev.output_sub_graph_1_pn_parc_marine2"))
    /** (BR_EDS, BR_ISR, IPR, BR_GAR, BR_TIE)  Left outer join ( Wasac, Acort, AllRap, Insee, ios_version_from_iai ) */

    val finalJoin = subGraph1.as('a).join(nbPairesJoinOut.as('b), subGraph1("ipr_idtcom") === nbPairesJoinOut("identifiant_1_produit"), "left_outer")
    /** Select the filed according to the target table PN_PARC_MARINE */

    val finalJoinOutput = finalJoin.select($"a.ipr_idtcom", $"a.eds_idteds", $"a.ipr_idtetaprd", $"b.rap", $"b.libelle_rap", $"a.ipr_lbtypprd", $"a.ipr_datmescom", $"b.ce_id", $"b.connexion_id", $"b.support_id", $"b.service_support", $"b.collect", $"b.support", $"b.nb_paires", $"b.collect_role", $"b.router_role", $"a.gar_lbcodgar", $"a.gar_lbplghor", $"a.tie_raiscltie", $"a.tie_addresstie", $"a.tie_codptlcomtie", $"a.tie_sirtie", $"a.ipr_steutlexta", $"a.ipr_comexta", $"a.ipr_voibpexta", $"a.ipr_codptlcomexta", $"a.ipr_steutlextb", $"a.ipr_comextb", $"a.ipr_voibpextb", $"a.ipr_codptlcomextb", $"b.population", $"b.population_cat", $"b.constructeur", $"b.chassis", $"b.ios_version", $"b.ios_version_from_iai", $"b.ip_admin", $"b.rsc", $"b.version_boot", $"b.dslam_id", $"b.master_dslam_id", $"b.nortel_id", $"b.pe_id", $"b.fav_id", $"b.ntu_id", $"b.tronc_type", $"b.num_pivot", $"b.element_reseau", $"b.type_interne")
    /** Rename the filed according to the target table PN_PARC_MARINE */

    val finalJoinOutputRename = finalJoinOutput.withColumnRenamed("ipr_idtcom", "identifiant_1_produit").withColumnRenamed("eds_idteds", "identifiant_eds_pilote").withColumnRenamed("ipr_idtetaprd", "etat_produit").withColumnRenamed("ipr_lbtypprd", "type_produit").withColumnRenamed("tie_raiscltie", "raison_sociale").withColumnRenamed("tie_sirtie", "siren").withColumnRenamed("ipr_voibpexta", "voie_extremite_A").withColumnRenamed("ipr_codptlcomexta", "cp_extremite_A").withColumnRenamed("ipr_comexta", "ville_extremite_A").withColumnRenamed("ipr_steutlexta", "societe_extremite_A").withColumnRenamed("ipr_steutlextb", "societe_extremite_B").withColumnRenamed("ipr_comextb", "ville_extremite_B").withColumnRenamed("ipr_voibpextb", "voie_extremite_B").withColumnRenamed("ipr_codptlcomextb", "cp_extremite_B").withColumnRenamed("gar_lbcodgar", "gtr").withColumnRenamed("gar_lbplghor", "plage_horaire_gtr").withColumnRenamed("tie_addresstie", "addresse_complete_client").withColumnRenamed("tie_codptlcomtie", "code_postal_client").withColumn("date_photo", current_date())

    /** Sort the output based on identifiant_1_produit */
    val finalJoinOutputSort = finalJoinOutputRename.orderBy("identifiant_1_produit")

    /** Remove the duplicate */
    /** Selecting the partition key */
    val Key = Window.partitionBy($"identifiant_1_produit")

    /** Group by identifiant_1_produit and assign row number  */
    val marineRemoveDuplicate = finalJoinOutputSort.select($"*", rowNumber.over(Key).alias("rn"))

    /** Group by identifiant_1_produit and order by row number in desc  */
    val key1 = Window.partitionBy($"identifiant_1_produit").orderBy(desc("rn"))

    /** Select the first column of Row number and drop row number field */
    val marineRemoveDuplicate1 = marineRemoveDuplicate.select($"*", rowNumber.over(key1).alias("rn_1")).where($"rn_1" === 1).drop("rn").drop("rn_1")
    val parcMarine = marineRemoveDuplicate1

    val p2 = parcMarine.withColumn("ipr_datmescom", Utils.tostring(parcMarine("ipr_datmescom")))
    println(p2.printSchema())
    /** Save the output in Hive location */

    //    logger.info("Writing parquet file for hive...")
    //    parcMarine.write.mode(SaveMode.Overwrite).parquet(Acquisition.getFilename("dev.hive_pn_parc_marine2"))
    //    logger.info("parquet file written")

    //    logger.info("Writing .CSV file ...")
    //    parcMarine.write.format("com.databricks.spark.csv").save(Acquisition.getFilename("dev.hive_pn_parc_marine2"))
    //    logger.info("CSV file written")

    logger.info("Writing .CSV file for hive...")
    Acquisition.writeFile(parcMarine, Acquisition.getFilename("dev.hive_pn_parc_marine2"), false)
    logger.info("CSV file written")

    logger.info("############ Ending phase 3 ############")
  }
}