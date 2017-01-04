package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import com.obs.pn.commons.Utils
import Utils.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object TransformEnrichI {
  val logger = LoggerFactory.getLogger(TransformEnrichI.getClass)

  /**
   * UDF to Replace ST from String with SAINT
   */
  val replaceSTToSaint = udf((s: String) => {
    if (s != null && s.startsWith("ST "))
      s.replace("ST ", "SAINT")
    else
      s
  })
  /**
   * Call the method that performs the transformation
   * @param fosavAcortAllRapRef DataFrame on which Transformation to be performed
   * @return villeExtremiteATransform Final Result after all transformation
   */
  def transform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    logger.debug("transform")
    val serviceSupport = serviceSupportTransform(fosavAcortAllRapRef)
    val supportTranf = supportTransform(serviceSupport)
    val collectTranf = collectTransform(supportTranf)
    val yearDateTrans = yearDateTransform(collectTranf)
    villeExtremiteATransform(yearDateTrans)
  }

  /**
   * Method performs Transformation on field "service_support"
   * @param fosavAcortAllRapRef DataFrame on which Transformation to be performed
   * @return fosavAcortAllRapRef Result of transformation
   */

  def serviceSupportTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    logger.debug("serviceSupportTransform")    
    fosavAcortAllRapRef.withColumn("service_support", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"support_type_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"support_type_secours").otherwise("NULL")).select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket", "date_cloture_ticket", "date_retablissement", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale", "raison_sociale_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description",
    "nature_finale", "responsabilite_pbm", "famille_de_probleme", "detail_probleme", "gtr", "plage_horaire_gtr", "societe_extremite_A",
    "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B", "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne", "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur_nom_utilisateur", "duree_gel", "duree_constractuelle_indispo", "nature_initiale", "nombre_der_clos_produit_3_mois", "dependance_ticket", "imputation_princ", "eds_active", "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "duree_totale", "pht_idtcom", "ip_admin", "ios_version", "constructeur", "chassis", "num_pivot", "element_reseau", "type_interne", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec", "identifiant_1_produit", "is_gtrisable", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "nb_paires", "collect_role", "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id", "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population", "population_cat", "rap", "service_support", "support_nominal", "support_secours", "collecte_nominal", "collecte_secours")
  }
  /**
   * Method performs Transformation on column "support"
   *  @param fosavAcortAllRapRef DataFrame on which Transformation to be performed
   *  @return fosavAcortAllRapRef Result of transformation
   */

  def supportTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    logger.debug("supportTransform")
    fosavAcortAllRapRef.withColumn("support", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"support_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"support_secours").otherwise("NULL")).
    select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket",
      "date_cloture_ticket", "date_retablissement", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale",
      "raison_sociale_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description",
      "nature_finale", "responsabilite_pbm", "famille_de_probleme", "detail_probleme", "gtr", "plage_horaire_gtr", "societe_extremite_A",
      "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B",
      "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne",
      "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur_nom_utilisateur", "duree_gel",
      "duree_constractuelle_indispo", "nature_initiale", "nombre_der_clos_produit_3_mois", "dependance_ticket", "imputation_princ", "eds_active",
      "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "duree_totale", "pht_idtcom", "ip_admin", "ios_version", "constructeur",
      "chassis", "num_pivot", "element_reseau", "type_interne", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec",
      "identifiant_1_produit", "is_gtrisable", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "nb_paires", "collect_role",
      "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id",
      "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population", "population_cat", "rap",
      "service_support", "support", "collecte_nominal", "collecte_secours")
  }
  /**
   * Method performs Transformation on column "collect"
   *  @param fosavAcortAllRapRef DataFrame on which Transformation to be performed
   *  @return fosavAcortAllRapRef Result of transformation
   */

  def collectTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    logger.debug("collectTransform")
    fosavAcortAllRapRef.withColumn("collect", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"collecte_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"collecte_secours").otherwise("NULL")).select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket", "date_cloture_ticket", "date_retablissement", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale", "raison_sociale_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description", "nature_finale", "responsabilite_pbm", "famille_de_probleme", "detail_probleme", "gtr", "plage_horaire_gtr", "societe_extremite_A", "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B", "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne", "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur_nom_utilisateur", "duree_gel", "duree_constractuelle_indispo", "nature_initiale", "nombre_der_clos_produit_3_mois", "dependance_ticket", "imputation_princ", "eds_active", "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "duree_totale", "pht_idtcom", "ip_admin", "ios_version", "constructeur", "chassis", "num_pivot", "element_reseau", "type_interne", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec", "identifiant_1_produit", "is_gtrisable", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "nb_paires", "collect_role", "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id", "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population", "population_cat", "rap", "service_support", "support", "collect")
  }
  /**
   * Tranformation to create a column "year_date" from "date_cloture_ticket"
   * @param fosavAcortAllRapRef DataFrame in which column to be added
   * @return fosavAcortAllRapRef
   */
  def yearDateTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {    
    logger.debug("yearDateTransform")
    fosavAcortAllRapRef.withColumn("year_date", substring($"date_cloture_ticket", 0, 4)).select($"*")
  }

  /**
   * Transformation to replace first occurrence of ST to SAINT in field ville_extremite_A using udf
   * @param fosavAcortAllRapRef DataFrame which is to be modified
   * @return fosavAcortAllRapRef
   */
  def villeExtremiteATransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    logger.debug("villeExtremiteATransform")
    fosavAcortAllRapRef.withColumn("ville_extremite_A", replaceSTToSaint($"ville_extremite_A"))
  }
  /**
   * Transforamtion to replace the column containing \n with ""
   * @param wasac replacing new line character with an empty string
   * @return a dataframe with empty string
   */
  def replaceWasac(wasac: DataFrame): DataFrame = {
    logger.debug("replaceWasac")
    wasac.withColumn("ios", regexp_replace($"ios", "[\\r\\n]", ""))
  }


}
