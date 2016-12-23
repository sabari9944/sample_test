package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.commons.Utils
import Utils.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TransformEnrichI {
  /**
   * Create Schema for gather transformation on field ville_extremite_A
   */
  case class villaSchema(num_ticket: String, ville_extremite_A: String)
  /**
   * Function to Replace ST with SAINT
   */
  val replaceSTToSaint = udf((s: String) => {
    if (s != null && s.startsWith("ST "))
      s.replace("ST ", "SAINT")
    else
      s
  })

  def transform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    val serviceSupport = serviceSupportTransform(fosavAcortAllRapRef)
    val supportTranf = supportTransform(serviceSupport)
    val collectTranf = collectTransform(supportTranf)
    val yearDateTrans = yearDateTransform(collectTranf)
    val allRapReferenceTransform = villeExtremiteATransform(yearDateTrans)
    return allRapReferenceTransform
  }

  /**
   * Transformation on field "service_support"
   */

  def serviceSupportTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    val serviceSupportColumn = fosavAcortAllRapRef.withColumn("service_support", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"support_type_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"support_type_secours").otherwise("NULL")).select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket", "date_cloture_ticket", "date_retablissement", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale", "raison_sociale_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description",
      "nature_finale", "responsabilite_pbm", "famille_de_probleme", "detail_probleme", "gtr", "plage_horaire_gtr", "societe_extremite_A",
      "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B", "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne", "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur_nom_utilisateur", "duree_gel", "duree_constractuelle_indispo", "nature_initiale", "nombre_der_clos_produit_3_mois", "dependance_ticket", "imputation_princ", "eds_active", "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "duree_totale", "pht_idtcom", "ip_admin", "ios_version", "constructeur", "chassis", "num_pivot", "element_reseau", "type_interne", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec", "identifiant_1_produit", "is_gtrisable", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "nb_paires", "collect_role", "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id", "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population", "population_cat", "rap", "service_support", "support_nominal", "support_secours", "collecte_nominal", "collecte_secours")

    return serviceSupportColumn
  }

  /**
   * Transformation on column "support"
   */

  def supportTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {

    val supportColumn = fosavAcortAllRapRef.withColumn("support", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"support_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"support_secours").otherwise("NULL")).
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

    return supportColumn

  }

  /**
   * Transformation on column collect
   */
  def collectTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    val collectColumn = fosavAcortAllRapRef.withColumn("collect", when($"router_role" === "NOMINAL" && $"router_role".isNotNull, $"collecte_nominal").when($"router_role" === "SECOURS" && $"router_role".isNotNull, $"collecte_secours").otherwise("NULL")).select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket", "date_cloture_ticket", "date_retablissement", "sem_cloture_ticket", "mois_cloture_ticket", "mois_cloture_ticket_indus", "raison_sociale", "raison_sociale_ticket", "addresse_complete_client", "code_postal_client", "siren", "datemescom", "type_produit", "etat_produit", "description", "nature_finale", "responsabilite_pbm", "famille_de_probleme", "detail_probleme", "gtr", "plage_horaire_gtr", "societe_extremite_A", "ville_extremite_A", "voie_extremite_A", "cp_extremite_A", "societe_extremite_B", "ville_extremite_B", "voie_extremite_B", "cp_extremite_B", "identifiant_2_produit", "identifiant_3_produit", "identifiant_4_produit", "gtr_respecte", "is_repetitif", "complement_interne", "libelle_imputation", "libelle_succint", "type_ticket", "donnee_complementaire", "initiateur_nom_utilisateur", "duree_gel", "duree_constractuelle_indispo", "nature_initiale", "nombre_der_clos_produit_3_mois", "dependance_ticket", "imputation_princ", "eds_active", "poste_active", "dt_deb_suivi", "dt_fin_suivi", "dt_deb_pec", "duree_totale", "pht_idtcom", "ip_admin", "ios_version", "constructeur", "chassis", "num_pivot", "element_reseau", "type_interne", "duree_indisponibilite", "duree_indisponibilite_hors_gel", "delai_activation_pec", "identifiant_1_produit", "is_gtrisable", "ce_id", "connexion_id", "support_id", "identifiant_sous_reseau", "nb_paires", "collect_role", "router_role", "bas_id", "category", "subcategory", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id", "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population", "population_cat", "rap", "service_support", "support", "collect")
    return collectColumn
  }

  def yearDateTransform(fosavAcortAllRapRef: DataFrame): DataFrame = {
    val yearDate = fosavAcortAllRapRef.withColumn("year_date", substring($"date_cloture_ticket", 0, 4)).select($"*")
    return yearDate
  }
  /**
   * Transformation to replace first occurrence of ST to SAINT in field ville_extremite_A
   */

  def villeExtremiteATransform(fosavAcortAllRapRef: DataFrame): DataFrame = {

    val villeExtremiteATransform = fosavAcortAllRapRef.withColumn("ville_extremite_A", replaceSTToSaint($"ville_extremite_A"))

    return villeExtremiteATransform
  }

}
