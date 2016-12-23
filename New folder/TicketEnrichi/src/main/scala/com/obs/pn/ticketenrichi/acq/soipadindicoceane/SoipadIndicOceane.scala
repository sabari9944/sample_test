package com.obs.pn.ticketenrichi.acq.soipadindicoceane

import org.apache.spark.sql.DataFrame
import com.obs.pn.ticketenrichi.acq.Acquisition
import com.obs.pn.commons.Utils
import Utils.sqlContext.implicits._

object SoipadIndicOceane {
  /**
   * Load SoipadIndicOceane File
   */
  def loadFile(): DataFrame = {
    val prop = Utils.prop
    val res = Acquisition.loadFileUnivocity(Utils.propFileLoader("dev.soipad"))
    return res
  }
  /**
   * Select the required columns after joining the file with SoipadIndicOceane
   */

  def transform(dataFrame: DataFrame): DataFrame = {
    val getData = dataFrame.select($"num_ticket", $"pht_idtcom", $"nombre_der_clos_produit_3_mois", $"is_repetitif", $"identifiant_1_produit", $"ce_id", $"connexion_id", $"support_id", $"identifiant_sous_reseau", $"service_support", $"collect", $"support", $"nb_paires", $"collect_role", $"type_produit", $"router_role", $"datemescom", $"etat_produit", $"identifiant_2_produit", $"identifiant_3_produit", $"identifiant_4_produit", $"identifiant_eds_pilote", $"poste_utilisateur", $"gtr", $"plage_horaire_gtr", $"gtr_respecte", $"origine", $"initiateur_nom_utilisateur", $"date_debut_ticket", $"date_creation_ticket", $"date_retablissement", $"date_cloture_ticket", $"duree_indisponibilite", $"duree_indisponibilite_hors_gel", $"duree_constractuelle_indispo", $"year_date", $"mois_cloture_ticket", $"mois_cloture_ticket_indus", $"sem_cloture_ticket", $"raison_sociale_ticket", $"raison_sociale", $"addresse_complete_client", $"code_postal_client", $"siren", $"societe_extremite_a", $"ville_extremite_a", $"voie_extremite_a", $"cp_extremite_a", $"societe_extremite_b", $"ville_extremite_b", $"voie_extremite_b", $"cp_extremite_b", $"bas_id", $"category", $"description", $"nature_initiale", $"nature_finale", $"responsabilite_pbm", $"famille_de_probleme", $"libelle_imputation", $"subcategory", $"detail_probleme", $"complement_interne", $"cause_nrgtr", $"responsabilite_nrgtr", $"donnee_complementaire", $"libelle_succint", $"constructeur", $"chassis", $"ios_version", $"ios_version_from_iai", $"ip_admin", $"dslam_id", $"master_dslam_id", $"nortel_id", $"pe_id", $"fav_id", $"ntu_id", $"tronc_type", $"is_gtrisable", $"population", $"population_cat", $"rap", $"type_ticket", $"dependance_ticket", $"imputation_princ", $"eds_active_1", $"eds_active_2", $"eds_active_3", $"eds_active_4", $"eds_active_5", $"poste_active", $"dt_deb_suivi", $"dt_fin_suivi", $"dt_deb_pec", $"delai_activation_pec", $"duree_gel", $"duree_totale", $"tic_dg_auto", $"num_pivot", $"element_reseau", $"type_interne")
    return getData
  }

}
