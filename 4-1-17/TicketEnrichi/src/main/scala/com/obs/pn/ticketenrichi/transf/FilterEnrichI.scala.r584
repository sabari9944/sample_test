package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rowNumber
import org.apache.spark.sql.types.StringType
import com.obs.pn.commons.Utils
import org.slf4j.LoggerFactory
import com.obs.pn.commons.Utils.sqlContext.implicits.StringToColumn

object FilterEnrichI {
  
  val logger = LoggerFactory.getLogger(FilterEnrichI.getClass)
/**
 * Sort the File based on the keyField.
 * 
 * @param dfAcort Unsorted DataFrame on which sorting is to be performed.
 * @return dfAcort Sorted DataFrame
 */
  def sortAcort(dfAcort: DataFrame): DataFrame = {
    logger.debug("sortAcort")
    dfAcort.orderBy("element_reseau")
  }

  /**
   * This method is call to perform filter transformation
   * on the dataframe after the join is performed on the fosav, wasac and acort file. 
   * 
   * @param acortFosavSelect DataFrame on joining OrderedAcort with FosavWasac
   * @param dfAcort Unsorted Acort
   * @param sortedAcort Sorted Acort
   * @return renameColumns DataFrame on which Select and Deselected transformation is performed
   */
  def transformAcort(acortFosavSelect: DataFrame, dfAcort: DataFrame, sortedAcort: DataFrame): DataFrame = {
    logger.debug("transformAcort")
    val acortSUDFFilter = filterSU(acortFosavSelect)
    val acortNONSUDFFilter = filterNonSU(acortFosavSelect)
    val ascortSuTransDF = suAddColumns(acortSUDFFilter)
    val lookupAcort = filterAcort(sortedAcort)
    val acortNonSuDF = nonSuTransf(acortNONSUDFFilter)
    val joinNonSU = joinNonSuLKPAcort(acortNonSuDF, lookupAcort)
    val acortAddColLKPAcort = nonSUAddColumnsLKPAcort(joinNonSU)
    val lookupTRONC = lkpTOPOTRONC(acortAddColLKPAcort, dfAcort)
    val lookupSi = lkpType(lookupTRONC, dfAcort)
    val acortFosavTrans = unionSuNonSU(ascortSuTransDF, lookupSi)
    renameColumns(acortFosavTrans)
  }

  /**
   * This method returns the DataFrame which is partitioned based on two columns and given ranking.
   * Data is filtered based on only the first rank to obtain a dataframe. Which is used as Lookup File
   * @param acort Sorted Acort on which filter is performed
   * @return acort DataFrame 
   */
  def filterAcort(acort: DataFrame): DataFrame = {
    logger.debug("filterAcort")
    val windowPartition = Window.partitionBy($"num_pivot", $"type_interne")
    acort.select($"*", rowNumber.over(windowPartition).alias("rn")).where($"rn" === 1)
  }

  /**
   * This method is used to perform filter on specific column and obtain DataFrame which matches the condition
   * @param acortBaseFilter DataFrame obtained by joining Sorted Acort with FosavWasac
   * @return acortSUDFNewCol DataFrame that matches filter Condition
   */
    def filterSU(acortBaseFilter: DataFrame): DataFrame = {
    logger.debug("filterSU")
    val acortSUDFNewCol = acortBaseFilter.withColumn("SU_", substring(acortBaseFilter("type_interne"), 1, 3) === "SU_").withColumn("SU_Not", substring(acortBaseFilter("type_interne"), 1, 3) !== "SU_")
    acortSUDFNewCol.filter(acortSUDFNewCol("SU_") === true)
  }
  
  /**
   * This method is used to perform filter on specific column and obtain DataFrame where condition is not matched
   * @param acortBaseFilter DataFrame obtained by joining Sorted Acort with FosavWasac
   * @return acortNONSUDFNewCol DataFrame where filter Condition is not matched.
   */
  def filterNonSU(acortBaseFilter: DataFrame): DataFrame = {
    logger.debug("filterNonSU")
    val acortNONSUDFNewCol = acortBaseFilter.withColumn("SU_", substring(acortBaseFilter("type_interne"), 1, 3) === "SU_").withColumn("SU_Not", substring(acortBaseFilter("type_interne"), 1, 3) !== "SU_")
	acortNONSUDFNewCol.filter(acortNONSUDFNewCol("SU_Not") === true || acortNONSUDFNewCol("SU_Not").isNull)
  }

  /**
   * The Method is used to perform all SU filter transformation. 
   * @param acortSUDFFilter DataFrame obtained from method filterSU
   * @return ascortSuTransDF5 DataFrame 
   */
  def suAddColumns(acortSUDFFilter: DataFrame): DataFrame = {
    logger.debug("suAddColumns")
    val windowPartition = Window.partitionBy($"num_ticket")
    val acortSUDFFilter1 = acortSUDFFilter.select($"*", rowNumber.over(windowPartition).alias("rn")).where($"rn" === 1).drop("rn")
    val ascortSuTransDF1 = acortSUDFFilter1.withColumn("duree_indisponibilite", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), Utils.dateDiffMin(Utils.getTimestamp(acortSUDFFilter1("date_retab_ticket")), Utils.getTimestamp(acortSUDFFilter1("date_debut_ticket")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(acortSUDFFilter1("date_cloture_ticket")), Utils.getTimestamp(acortSUDFFilter1("date_debut_ticket"))))).otherwise("0")))
    val ascortSuTransDF2 = ascortSuTransDF1.withColumn("duree_indisponibilite_hors_gel", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_retab_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_cloture_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise("0")))
    val ascortSuTransDF3 = ascortSuTransDF2.withColumn("delai_activation_pec", when(($"dt_deb_suivi".isNotNull) && ($"dt_deb_pec".isNotNull) && ($"dt_deb_pec").gt($"dt_deb_suivi"), (Utils.dateDiffMin(Utils.getTimestamp(ascortSuTransDF2("dt_deb_pec")), Utils.getTimestamp(ascortSuTransDF2("dt_deb_suivi"))))).otherwise("0"))
    val ascortSuTransDF4 = ascortSuTransDF3.withColumn("duree_totale", when(($"dt_fin_cause".isNull) && ($"dt_deb_cause".isNull), "0").otherwise(Utils.dateDiffMin(Utils.getTimestamp(ascortSuTransDF3("dt_fin_cause")), Utils.getTimestamp(ascortSuTransDF3("dt_deb_cause")))))
    val ascortSuTransDF5 = ascortSuTransDF4.withColumn("num_pivot", lit("": String))
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
	ascortSuTransDF5.select("num_ticket", "usc", "identifiant_eds", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket", "date_cloture_ticket", "date_retab_ticket", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale_client", "raison_sociale_client_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description", "nature_finale", "responsabilite_pbm", "famille_pbm", "detail_pbm", "feuillet", "GTR", "plage_horaire_gtr", "societe_extremite_A", "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B", "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne", "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur", "duree_gel", "duree_constractuelle_indispo", "nature_initiale", "nb_der_clos_pdt_3_mois", "dependance_ticket", "imputation_princ", "eds_active", "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "dt_deb_cause", "dt_fin_cause", "ip_admin", "type_routeur", "ios", "constructeur", "chassis", "SU_", "SU_Not", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec", "duree_totale", "is_gtrisable", "num_pivot", "element_reseau", "type_interne", "support", "role", "type_si", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "service_support", "collect", "nb_paires", "collect_role", "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "master_dslam_id", "dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "population", "population_cat", "year_date", "tronc_type", "rap")

  }
  
  /**
   * The Method is used to perform all Non SU filter transformation. 
   * @param acortNONSUDFFilter DataFrame obtained from method filterNonSU
   * @return acortNonSuTransDF3 DataFrame 
   */
  def nonSuTransf(acortNONSUDFFilter: DataFrame): DataFrame = {
    import Utils.sqlContext.implicits._
    logger.debug("nonSuTransf")
    val acortNonSuTransDF1 = acortNONSUDFFilter.withColumn("duree_indisponibilite", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), Utils.dateDiffMin(Utils.getTimestamp(acortNONSUDFFilter("date_retab_ticket")), Utils.getTimestamp(acortNONSUDFFilter("date_debut_ticket")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(acortNONSUDFFilter("date_cloture_ticket")), Utils.getTimestamp(acortNONSUDFFilter("date_debut_ticket"))))).otherwise("0")))
    val acortNonSuTransDF2 = acortNonSuTransDF1.withColumn("duree_indisponibilite_hors_gel", when(($"date_retab_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_retab_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise(when(($"date_cloture_ticket".isNotNull) && ($"date_debut_ticket".isNotNull) && ($"duree_gel".isNotNull), (Utils.dateDiffMin(Utils.getTimestamp(($"date_cloture_ticket")), Utils.getTimestamp(($"date_debut_ticket"))) - (($"duree_gel")))).otherwise("0")))
    val acortNonSuTransDF3 = acortNonSuTransDF2.withColumn("delai_activation_pec", when(($"dt_deb_suivi".isNotNull) && ($"dt_deb_pec".isNotNull) && ($"dt_deb_pec").gt($"dt_deb_suivi"), (Utils.dateDiffMin(Utils.getTimestamp(($"dt_deb_pec")), Utils.getTimestamp(($"dt_deb_suivi"))))).otherwise("0"))
      .withColumn("duree_totale", when(($"dt_fin_cause".isNull) || ($"dt_deb_cause".isNull), "0").otherwise(Utils.dateDiffMin(Utils.getTimestamp(($"dt_fin_cause")), Utils.getTimestamp(($"dt_deb_cause")))))
    acortNonSuTransDF3.withColumn("is_gtrisable", when(($"type_ticket" === "Incident") && ($"responsabilite_pbm" === "Groupe Orange") && (($"nature_initiale" === "SERVICE INTERROMPU") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVÉ") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIVE") || ($"nature_initiale" === "SERVICE AVEC SECOURS ACTIV?")), "Oui").otherwise("Non"))
  }
/**
 * Perform join on the Acort LookUp File
 * @param acortNonSuDF DataFrame on which transformation is applied on Non SU filtered Dataframe
 * @param lookupAcort Look File created from Acort File 
 * @return acortNonSuDF DataFrame
 */
  def joinNonSuLKPAcort(acortNonSuDF: DataFrame, lookupAcort: DataFrame): DataFrame = {    
    logger.debug("joinNonSuLKPAcort")
    acortNonSuDF.as('a).join(lookupAcort.as('b), $"a.num_pivot" === $"b.num_pivot" && $"a.type_interne" === $"b.type_interne", "left_outer").drop(lookupAcort("num_pivot")).drop(lookupAcort("element_reseau")).drop(lookupAcort("num_pivot")).drop(lookupAcort("type_interne")).drop(lookupAcort("rn")).drop(lookupAcort("type_si")).drop(lookupAcort("role")).drop("debit")
  }

  /**
   * Perform Lookup transformation on Non SU filtered DataFrame
   * @param acortNONSUDFFilter DataFrame by join Non Su filtered DataFrame with Lookup 
   * @return acortNonSuTransDF14 DataFrame with transformation
   */
  def nonSUAddColumnsLKPAcort(acortNONSUDFFilter: DataFrame): DataFrame = {
    import Utils.sqlContext.implicits._
    logger.debug("nonSUAddColumnsLKPAcort")
    val acortNonSuTransDF5 = acortNONSUDFFilter.withColumn("ce_id", when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP160", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "R_TRL", $"element_reseau").otherwise("null")))
    val acortNonSuTransDF6 = acortNonSuTransDF5.withColumn("connexion_id", when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP164", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP180", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP220", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP210", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP165", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "A_AFR", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "A_PHD", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "A_PHD", $"element_reseau").otherwise("NULL")))))))))
    val acortNonSuTransDF7 = acortNonSuTransDF6.withColumn("support_id", when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP163", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP178", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP209", $"element_reseau").otherwise(when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP180", $"element_reseau").otherwise("NULL")))))
    val acortNonSuTransDF8 = acortNonSuTransDF7.withColumn("identifiant_sous_reseau", lit(null: String).cast(StringType))
      .withColumn("service_support", lit(null: String).cast(StringType))
      .withColumn("collect", lit(null: String).cast(StringType))
      .withColumn("support", lit(null: String).cast(StringType))
      .withColumn("nb_paires", lit(null: String).cast(StringType))
      .withColumn("collect_role", lit(null: String).cast(StringType))
    val acortNonSuTransDF9 = acortNonSuTransDF8.withColumn("router_role", when($"type_interne".isNotNull && $"type_interne" === "RSS_TYP160", $"role").otherwise("NULL"))
    val acortNonSuTransDF10 = acortNonSuTransDF9.withColumn("bas_id", lit(null: String).cast(StringType))
      .withColumn("category", lit(null: String).cast(StringType))
      .withColumn("subcategory", lit(null: String).cast(StringType))
      .withColumn("cause_nrgtr", lit(null: String).cast(StringType))
      .withColumn("responsabilite_nrgtr", lit(null: String).cast(StringType))
      .withColumn("ios_version_from_iai", lit(null: String).cast(StringType)).withColumn("master_dslam_id", lit(null: String).cast(StringType))
    val acortNonSuTransDF11 = acortNonSuTransDF10.withColumn("dslam_id", when($"type_interne".isNotNull && $"type_interne" === "TOPO_DSLAM", $"element_reseau").otherwise("NULL"))
    val acortNonSuTransDF12 = acortNonSuTransDF11.withColumn("nortel_id", when($"type_interne".isNotNull && $"type_interne" === "TOPO_NORTEL", $"element_reseau").otherwise("NULL"))
    val acortNonSuTransDF13 = acortNonSuTransDF12.withColumn("pe_id", when($"type_interne".isNotNull && $"type_interne" === "TOPO_PE", $"element_reseau").otherwise("NULL"))
    val acortNonSuTransDF14 = acortNonSuTransDF13.withColumn("fav_id", when($"type_interne".isNotNull && $"type_interne" === "TOPO_FAV", $"element_reseau").otherwise("NULL"))
    acortNonSuTransDF14.withColumn("ntu_id", when($"type_interne".isNotNull && $"type_interne" === "TOPO_NTU", $"element_reseau").otherwise("NULL"))
  }
  
  /**
   *Lookup File TOPOTRONC for Transformation on Non SU Filtered DataFrame 
   * @param acortAddColLKPAcort Non Su DataFrame on which transformation are performed
   * @param dfAcort unsorted Acort 
   * return acortNonSuTransfLKP DataFrame with transformation
   */

  def lkpTOPOTRONC(acortAddColLKPAcort: DataFrame, dfAcort: DataFrame): DataFrame = {
    import Utils.sqlContext.implicits._
    logger.debug("lkpTOPOTRONC")
    val windowPartition = Window.partitionBy($"num_pivot")
    var lkpTOPOTRONC = (dfAcort.filter(dfAcort("type_interne").startsWith("TOPO_TRONC")))
    lkpTOPOTRONC = lkpTOPOTRONC.select($"num_pivot" as "TRONC_num_pivot", $"element_reseau" as "TRONC_element_reseau", $"type_interne" as "TRONC_type_interne", rowNumber.over(windowPartition).alias("rn")).where($"rn" === 1)
    val joinWithLkp = acortAddColLKPAcort.as('a).join(lkpTOPOTRONC.as('b), $"a.num_pivot" === $"b.TRONC_num_pivot", "left_outer")
    val acortNonSuTransfLKP = joinWithLkp.withColumn("tronc_type", when(($"TRONC_type_interne".isNotNull) || ($"TRONC_type_interne" !== "") || ($"TRONC_type_interne" !== null) || ($"TRONC_type_interne" !== "null"), $"TRONC_type_interne").otherwise("NULL")).drop("rn")
	acortNonSuTransfLKP.withColumn("population", lit(null: String).cast(StringType)).withColumn("population_cat", lit(null: String).cast(StringType)).withColumn("year_date", lit(null: String).cast(StringType))
  }

 
  /**
   * Lookup File TYPE_SI and SISU for Transformation on Non SU Filtered DataFrame
   * @param acortAddColLKPAcort Non Su DataFrame on which transformation are performed
   * @param dfAcort unsorted Acort 
   * return joinAcortWithLKP DataFrame on Non SU Filtered Data
   */
  
  def lkpType(acortAddColLKPAcort: DataFrame, dfAcort: DataFrame): DataFrame = {
    logger.debug("lkpType")
    val windowPartition = Window.partitionBy($"num_pivot")
	
    //  Lookup file si_SU
    var lkpSiSU = (dfAcort.filter(dfAcort("type_si") === "S-SU"))
    lkpSiSU = lkpSiSU.select($"num_pivot" as "SISU_num_pivot", $"element_reseau" as "SISU_element_reseau", $"type_interne" as "SISU_type_interne", rowNumber.over(windowPartition).alias("rn")).where($"rn" === 1).drop("rn")

    //  Lookup file TYPE_SI
    var lkpTYPESI = (dfAcort.filter(dfAcort("type_si").startsWith("A-Z")))
    lkpTYPESI = lkpTYPESI.select($"num_pivot" as "TYPE_SI_num_pivot", $"element_reseau" as "TYPE_SI_element_reseau", $"type_interne" as "TYPE_SI_type_interne", rowNumber.over(windowPartition).alias("rn")).where($"rn" === 1).drop("rn")

    val joinLKP = lkpSiSU.as('a).join(lkpTYPESI.as('b), $"a.SISU_num_pivot" === $"b.TYPE_SI_num_pivot", "full_outer")
    val joinAcortWithLKP = acortAddColLKPAcort.as('a).join(joinLKP.as('b), $"a.num_pivot" === $"b.SISU_num_pivot" || $"a.num_pivot" === $"b.TYPE_SI_num_pivot", "left_outer")

	joinAcortWithLKP.withColumn("rap", when(($"SISU_type_interne".isNotNull) || ($"SISU_type_interne" !== "") || ($"SISU_type_interne" !== null) || ($"SISU_type_interne" !== "null"), (substring($"SISU_type_interne", 4, 10))).otherwise(when(($"TYPE_SI_type_interne".isNotNull) || ($"TYPE_SI_type_interne" !== "") || ($"TYPE_SI_type_interne" !== null) || ($"TYPE_SI_type_interne" !== "null"), "TYPE_SI_type_interne").otherwise("NULL")))
      .drop("TRONC_num_pivot").drop("TRONC_element_reseau").drop("TRONC_type_interne")
      .drop("SISU_num_pivot").drop("SISU_element_reseau").drop("SISU_type_interne")
      .drop("TYPE_SI_num_pivot").drop("TYPE_SI_element_reseau").drop("TYPE_SI_type_interne").drop("rn")
  }

  /**
   * Returns the dataframe by performing on the two dataframes obtained from SU and NON SU filteration
   * @param ascortSuTransDF Su Filtered and Transformed DataFrame
   * @param acortNonSuTransDF NonSu Filtered and Transformed DataFrame
   * @return acortNonSuTransDF Union of two DataFrames
   */  
  def unionSuNonSU(ascortSuTransDF: DataFrame, acortNonSuTransDF: DataFrame): DataFrame = {
    logger.debug("lkpType")
    acortNonSuTransDF.unionAll(ascortSuTransDF)
  }

  /**
   * This method is used to rename column of some of the field from the dataframe
   * @param acortFosavTrans DataFrame Containing both Su and NonSu union result
   * @return acortFosavTrans
   */
  def renameColumns(acortFosavTrans: DataFrame): DataFrame = {
    logger.debug("lkpType")
    acortFosavTrans.withColumn("pht_idtcom", $"feuillet").withColumnRenamed("identifiant_eds", "identifiant_eds_pilote")
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
  }

}
