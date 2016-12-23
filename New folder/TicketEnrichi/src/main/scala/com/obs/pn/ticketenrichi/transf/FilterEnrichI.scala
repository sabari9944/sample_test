package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import com.obs.pn.commons.Utils
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import Utils.sqlContext.implicits._

object FilterEnrichI {
  /**
   * Call the methods to perform Transformation
   */
  def transformAcort(acortFosavSelect: DataFrame, dfAcort: DataFrame): DataFrame = {
    val acortBaseFilter = lookUP(acortFosavSelect, dfAcort)
    val acortSUDFFilter = filterSU(acortBaseFilter)
    val acortNONSUDFFilter = filterNonSU(acortBaseFilter)
    val ascortSuTransDF = suAddColumns(acortSUDFFilter)
    val acortNonSuTransDF = nonSUAddColumns(acortNONSUDFFilter, acortBaseFilter)
    val acortFosavTrans = unionSuNonSU(ascortSuTransDF, acortNonSuTransDF)
    val resultTrans = renameColumns(acortFosavTrans)

    return resultTrans
  }

  /**
   * Filter distinct values
   */
  def filterAcort(acort: DataFrame): DataFrame = {
    val acortRollUp = acort.select("*").groupBy("num_pivot").agg(first("element_reseau") as "element_reseau", first("type_interne") as "type_interne", first("type_si") as "type_si", first("role") as "role")
    acortRollUp
  }

  /**
   * Loop up logic for TOTP_TRONC,Su,SI,TYPE_SI
   */

  def lookUP(acortFosavSelect: DataFrame, dfAcort: DataFrame): DataFrame = {
    //  Lookup file TRONC
    val TOPO_TRONC = (acortFosavSelect.filter(dfAcort("type_interne").
      startsWith("TOPO_TRONC")))

    //  Lookup file si_SU
    val si_SU = (acortFosavSelect.filter(dfAcort("type_si") === "S-SU"))

    //  Lookup file TYPE_SI
    val TYPE_SI = (acortFosavSelect.filter(dfAcort("type_si").startsWith("A-Z")))

    //Selecting (filtering) only required columns from lookup file for join
    val lk_TRONC = TOPO_TRONC.select(
      TOPO_TRONC("num_pivot") as ("TRONC_num_pivot"),
      TOPO_TRONC("element_reseau") as ("TRONC_element_reseau"),
      TOPO_TRONC("type_interne") as ("TRONC_type_interne"))

    val lk_si_SU = si_SU.select(
      si_SU("num_pivot") as ("SISU_num_pivot"),
      si_SU("element_reseau") as ("SISU_element_reseau"),
      si_SU("type_interne") as ("SISU_type_interne"))

    val lk_TYPE_SI = TYPE_SI.select(
      TYPE_SI("num_pivot") as ("TYPE_SI_num_pivot"),
      TYPE_SI("element_reseau") as ("TYPE_SI_element_reseau"),
      TYPE_SI("type_interne") as ("TYPE_SI_type_interne"))

    // De-selected Input file join with lk_TRONC
    val join2 = acortFosavSelect.join(lk_TRONC, acortFosavSelect("num_pivot") === lk_TRONC("TRONC_num_pivot"), "left_outer")

    // De-selected Input file join with lk_lk_si_SU
    val join3 = join2.join(lk_si_SU, join2("num_pivot") === lk_si_SU("SISU_num_pivot"), "left_outer")

    // De-selected Input file join with lk_TYPE_S
    val join4 = join3.join(lk_TYPE_SI, join3("num_pivot") === lk_TYPE_SI("TYPE_SI_num_pivot"), "left_outer")

    val acortTypInternArr = dfAcort.groupBy(dfAcort("num_pivot") as "accort_num_pivot")
      .agg(expr("collect_list(type_interne) AS type_interne_array"),
        expr("collect_list(element_reseau) AS element_reseau_array"))

    val acortBaseFilter = join4.join(acortTypInternArr, join4("num_pivot") === acortTypInternArr("accort_num_pivot"),
      "left_outer")

    return acortBaseFilter
  }

  /**
   * Filter for SU_ logic
   */

  def filterSU(acortBaseFilter: DataFrame): DataFrame = {

    val acortSUDFNewCol = acortBaseFilter.withColumn("SU_", substring(acortBaseFilter("type_interne"), 0, 2) === "SU_").withColumn("SU_Not", substring(acortBaseFilter("type_interne"), 0, 2) !== "SU_")
    val acortSUDFFilter = acortSUDFNewCol.filter(acortSUDFNewCol("SU_") === true)
    return acortSUDFFilter
  }

  /**
   * Filter for !SU_ logic
   */
  def filterNonSU(acortBaseFilter: DataFrame): DataFrame = {

    val acortNONSUDFNewCol = acortBaseFilter.withColumn("SU_", substring(acortBaseFilter("type_interne"), 0, 2) === "SU_").withColumn("SU_Not", substring(acortBaseFilter("type_interne"), 0, 2) !== "SU_")
    val acortNONSUDFFilter = acortNONSUDFNewCol.filter(acortNONSUDFNewCol("SU_Not") === true || acortNONSUDFNewCol("SU_Not").isNull)
    return acortNONSUDFFilter
  }

  /**
   * Transformation logic for SU_ filter
   */
  def suAddColumns(acortSUDFFilter: DataFrame): DataFrame = {
    val ascortSuTransDF1 = acortSUDFFilter.withColumn("duree_indisponibilite", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), Utils.dateDiffMin(Utils.getTimestamp(acortSUDFFilter("date_retab_ticket")), Utils.getTimestamp(acortSUDFFilter("date_debut_ticket")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(acortSUDFFilter("date_cloture_ticket")), Utils.getTimestamp(acortSUDFFilter("date_debut_ticket"))))).otherwise("0")))
    val ascortSuTransDF2 = ascortSuTransDF1.withColumn("duree_indisponibilite_hors_gel", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_retab_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_cloture_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise("0")))
    val ascortSuTransDF3 = ascortSuTransDF2.withColumn("delai_activation_pec", when(($"dt_deb_suivi".isNotNull) && ($"dt_deb_pec".isNotNull) && ($"dt_deb_pec").gt($"dt_deb_suivi"), (Utils.dateDiffMin(Utils.getTimestamp(ascortSuTransDF2("dt_deb_pec")), Utils.getTimestamp(ascortSuTransDF2("dt_deb_suivi"))))).otherwise("0"))
    val ascortSuTransDF4 = ascortSuTransDF3.withColumn("duree_totale", when(($"dt_fin_cause".isNull) && ($"dt_deb_cause".isNull), "0").otherwise(Utils.dateDiffMin(Utils.getTimestamp(ascortSuTransDF3("dt_fin_cause")), Utils.getTimestamp(ascortSuTransDF3("dt_deb_cause")))))
    val ascortSuTransDF = ascortSuTransDF4.withColumn("num_pivot", lit("": String))
      .withColumn("element_reseau", lit("": String))
      .withColumn("type_interne", lit("": String))
      .withColumn("is_gtrisable", when(($"type_ticket" === "Incident") && ($"responsabilite_pbm" === "Groupe Orange") && (($"nature_initiale" === "SERVICE INTERROMPU") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVÉ") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVE") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIV?")), "Oui").otherwise("Non"))
      .withColumn("ce_id", lit("": String))
      .withColumn("connexion_id", lit("": String))
      .withColumn("support_id", lit("": String))
      .withColumn("identifiant_sous_reseau", lit("": String))
      .withColumn("service_support", lit("": String))
      .withColumn("collect", lit("": String))
      .withColumn("support", lit("": String))
      .withColumn("nb_paires", lit("": String))
      .withColumn("collect_role", lit("": String))
      .withColumn("router_role", lit("": String))
      .withColumn("bas_id", lit("": String))
      .withColumn("category", lit("": String))
      .withColumn("subcategory", lit("": String))
      .withColumn("cause_nrgtr", lit("": String))
      .withColumn("responsabilite_nrgtr", lit("": String))
      .withColumn("ios_version_from_iai", lit("": String))
      .withColumn("dslam_id", lit("": String))
      .withColumn("master_dslam_id", lit("": String))
      .withColumn("nortel_id", lit("": String))
      .withColumn("pe_id", lit("": String))
      .withColumn("fav_id", lit("": String))
      .withColumn("ntu_id", lit("": String))
      .withColumn("tronc_type", lit("": String))
      .withColumn("population", lit("": String))
      .withColumn("population_cat", lit("": String))
      .withColumn("year_date", lit("": String))
      .withColumn("rap", lit("": String))
    return ascortSuTransDF
  }
  /**
   * Transformation logic for Non SU_ filter
   */

  def nonSUAddColumns(acortNONSUDFFilter: DataFrame, acortBaseFilter: DataFrame): DataFrame = {
    import Utils.sqlContext.implicits._
    val acortNonSuTransDF1 = acortNONSUDFFilter.withColumn("duree_indisponibilite", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), Utils.dateDiffMin(Utils.getTimestamp(acortNONSUDFFilter("date_retab_ticket")), Utils.getTimestamp(acortNONSUDFFilter("date_debut_ticket")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(acortNONSUDFFilter("date_cloture_ticket")), Utils.getTimestamp(acortNONSUDFFilter("date_debut_ticket"))))).otherwise("0")))
    val acortNonSuTransDF2 = acortNonSuTransDF1.withColumn("duree_indisponibilite_hors_gel", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_retab_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_cloture_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise("0")))
    val acortNonSuTransDF3 = acortNonSuTransDF2.withColumn("delai_activation_pec", when(($"dt_deb_suivi".isNotNull) && ($"dt_deb_pec".isNotNull) && ($"dt_deb_pec").gt($"dt_deb_suivi"), (Utils.dateDiffMin(Utils.getTimestamp(($"dt_deb_pec")), Utils.getTimestamp(($"dt_deb_suivi"))))).otherwise("0"))
      .withColumn("duree_totale", when(($"dt_fin_cause".isNull) || ($"dt_deb_cause".isNull), "0").otherwise(Utils.dateDiffMin(Utils.getTimestamp(($"dt_fin_cause")), Utils.getTimestamp(($"dt_deb_cause")))))
    val acortNonSuTransDF4 = acortNonSuTransDF3.withColumn("is_gtrisable", when(($"type_ticket" === "Incident") && ($"responsabilite_pbm" === "Groupe Orange") && (($"nature_initiale" === "SERVICE INTERROMPU") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVÉ") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVE") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIV?")), "Oui").otherwise("Non"))
    val acortNonSuTransDF5 = acortNonSuTransDF4.withColumn("ce_id", Utils.calculateCeId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF6 = acortNonSuTransDF5.withColumn("connexion_id", Utils.calculateConnexionId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF7 = acortNonSuTransDF6.withColumn("support_id", Utils.calculateSupportId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF8 = acortNonSuTransDF7.withColumn("identifiant_sous_reseau", lit("": String))
      .withColumn("service_support", lit("": String))
      .withColumn("collect", lit("": String))
      .withColumn("support", lit("": String))
      .withColumn("nb_paires", lit("": String))
      .withColumn("collect_role", lit("": String))
    val acortNonSuTransDF9 = acortNonSuTransDF8.withColumn("router_role", Utils.calculateRouterRole(acortBaseFilter("type_interne"), acortBaseFilter("role")))
    val acortNonSuTransDF10 = acortNonSuTransDF9.withColumn("bas_id", lit("": String))
      .withColumn("category", lit("": String))
      .withColumn("subcategory", lit("": String))
      .withColumn("cause_nrgtr", lit("": String))
      .withColumn("responsabilite_nrgtr", lit("": String))
      .withColumn("ios_version_from_iai", lit("": String)).withColumn("master_dslam_id", lit("": String))
    val acortNonSuTransDF11 = acortNonSuTransDF10.withColumn("dslam_id", Utils.calculateDslamId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF12 = acortNonSuTransDF11.withColumn("nortel_id", Utils.calculateNortelId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF13 = acortNonSuTransDF12.withColumn("pe_id", Utils.calculatePeId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF14 = acortNonSuTransDF13.withColumn("fav_id", Utils.calculateFavId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF15 = acortNonSuTransDF14.withColumn("ntu_id", Utils.calculateNtuId(acortBaseFilter("type_interne_array"), acortBaseFilter("element_reseau_array")))
    val acortNonSuTransDF16 = acortNonSuTransDF15.withColumn("tronc_type", Utils.calculateTroncType(acortBaseFilter("type_interne_array")))
    val acortNonSuTransDF17 = acortNonSuTransDF16.withColumn("population", lit("": String))
      .withColumn("population_cat", lit("": String))
      .withColumn("year_date", lit("": String))
    val acortNonSuTransDF = acortNonSuTransDF17.withColumn("rap", Utils.calculateRap(acortBaseFilter("SISU_type_interne"), acortBaseFilter("TYPE_SI_type_interne")))

    return acortNonSuTransDF

  }

  /**
   * Union SU_ and Non SU_ transformations
   */
  def unionSuNonSU(ascortSuTransDF: DataFrame, acortNonSuTransDF: DataFrame): DataFrame = {

    val acortFosavTrans = acortNonSuTransDF.unionAll(ascortSuTransDF)

    return acortFosavTrans
  }

  /**
   * Rename columns from transformations
   */
  def renameColumns(acortFosavTrans: DataFrame): DataFrame = {
    val acortFosavTransRename = acortFosavTrans.withColumn("pht_idtcom", $"feuillet").withColumnRenamed("identifiant_eds", "identifiant_eds_pilote")
      .withColumnRenamed("date_retab_ticket", "date_retablissement")
      .withColumnRenamed("raison_sociale_client", "raison_sociale")
      .withColumnRenamed("raison_sociale_client_ticket", "raison_sociale_ticket")
      .withColumnRenamed("detail_pbm", "detail_probleme")
      .withColumnRenamed("famille_pbm", "famille_de_probleme")
      .withColumnRenamed("GTR", "gtr")
      .withColumnRenamed("nb_der_clos_pdt_3_mois", "nombre_der_clos_produit_3_mois")
      .withColumnRenamed("ios", "ios_version")
      .withColumnRenamed("feuillet", "identifiant_1_produit")
      .withColumnRenamed("initiateur", "initiateur_nom_utilisateur")

    return acortFosavTransRename
  }

}
