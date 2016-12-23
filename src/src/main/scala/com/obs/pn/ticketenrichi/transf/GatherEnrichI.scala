package com.obs.pn.ticketenrichi.transf

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.ticketenrichi.commons.Utils
import org.apache.spark.sql.functions.lit
import Utils.sqlContext.implicits._

object GatherEnrichI {
  def transform(dataFrame: DataFrame, categoriesIncidents: DataFrame): DataFrame = {
    val renameCol = renameCategoriesIncidentCol(categoriesIncidents)
    val unionOfJoin = JoinCategories(dataFrame, renameCol)
    val categoriesTransform = dropColJoinResult(unionOfJoin)
    return categoriesTransform
  }

  def renameCategoriesIncidentCol(categoriesIncidents: DataFrame): DataFrame = {
    val renameCol = categoriesIncidents.withColumnRenamed("CATEGORIE", "category").withColumnRenamed("SOUS-CATEGORIE", "subcategory")
    return renameCol
  }

  /**
   * Joining the DataFrame using inner join and using unused port
   */
  def JoinCategories(dataFrame: DataFrame, categoriesIncidents: DataFrame): DataFrame = {
    val categoriesJoin = dataFrame.as('a).join(categoriesIncidents.as('b), $"a.detail_probleme" === $"b.DETAIL PROBLEME", "inner")

    val unUsedCategoriesJoin = dataFrame.as('a).join(categoriesIncidents.as('b), $"a.detail_probleme" === $"b.DETAIL PROBLEME", "left_outer").where($"b.DETAIL PROBLEME".isNull)

    val categoriesJoinUnUsedPort = unUsedCategoriesJoin.as('a).join(categoriesIncidents.as('b), $"a.famille_de_probleme" === $"b.FAMILLE PROBLEME", "inner").select($"a.*")
    val unusedOfUnUsedPort = unUsedCategoriesJoin.as('a).join(categoriesIncidents.as('b), $"a.famille_de_probleme" === $"b.FAMILLE PROBLEME", "left_outer").where($"b.FAMILLE PROBLEME".isNull).select($"a.*")

    val categorySubCategoryTransf = unusedOfUnUsedPort.withColumn("category", lit("Autres")).withColumn("subcategory", lit("Autres"))
    val unionResult = categoriesJoin.unionAll(categoriesJoinUnUsedPort).unionAll(categorySubCategoryTransf).select("num_ticket", "identifiant_eds_pilote", "poste_utilisateur", "origine", "date_creation_ticket", "date_debut_ticket",
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
      "router_role", "bas_id", "cause_nrgtr", "responsabilite_nrgtr", "ios_version_from_iai", "dslam_id",
      "master_dslam_id", "nortel_id", "pe_id", "fav_id", "ntu_id", "tronc_type", "population_cat", "rap", "service_support", "support", "collect", "year_date", "population", "category", "subcategory")

    val distinctResult = unionResult//.distinct()
    return distinctResult

  }

  def dropColJoinResult(dataFrame: DataFrame): DataFrame = {
    val categoryTransf = dataFrame.drop("cause_nrgtr").drop("responsabilite_nrgtr").select("*")
    return categoryTransf
  }

}
