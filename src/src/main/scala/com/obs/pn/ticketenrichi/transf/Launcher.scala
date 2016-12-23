package com.obs.pn.ticketenrichi.transf

import java.io.FileNotFoundException
import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rowNumber
import org.slf4j.LoggerFactory
import com.obs.pn.ticketenrichi.acq.Acort
import com.obs.pn.ticketenrichi.acq.allrapreference.AllRapReference
import com.obs.pn.ticketenrichi.acq.categoriesincidents.CategoriesIncidents
import com.obs.pn.ticketenrichi.acq.communesinseeutile.CommunesInseeUtile
import com.obs.pn.ticketenrichi.acq.correspondancecodeinseecodepostalutile.CorrespondanceCodeInseecodePostalUtile
import com.obs.pn.ticketenrichi.acq.incidentologiefosav.IncidentologieFosav
import com.obs.pn.ticketenrichi.acq.incidentologiewasac.IncidentologieWasac
import com.obs.pn.ticketenrichi.acq.incidentologiewasaciai.IncidentologieWasacIai
import com.obs.pn.ticketenrichi.acq.lkpactivations.LKPActivations
import com.obs.pn.ticketenrichi.acq.lkprepetitif.LKPRepetitif
import com.obs.pn.ticketenrichi.acq.nrgtrreferentiel.NRGTRReferentiel
import com.obs.pn.ticketenrichi.acq.soipadindicoceane.SoipadIndicOceane
import com.obs.pn.ticketenrichi.commons.Utils
import com.obs.pn.ticketenrichi.commons.Utils.sqlContext.implicits.StringToColumn
import com.obs.pn.ticketenrichi.commons.Utils.sqlContext.implicits.localSeqToDataFrameHolder
import com.obs.pn.ticketenrichi.commons.TicketEnrichiConstants
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

object Launcher {
  /**
   * Launcher is the driver object to drive flow of Enrichi
   * Enrich flow is divided basically in to Filter,Transform and gather parts
   */

  def main(args: Array[String]) {
    /**
     *  Read data parameter from command line
     */

    if (args.length < 2) {
      /**
       * System validation for date parameter, this is system err not println
       */
      System.err.println("Launch Parameter Required:  <Date:yyyymmdd> <HDFSPATH: Config File> ")
      System.exit(1)
    } else {
      Utils.runDate = args(0)
      Utils.pathToConf = args(1)
      Utils.prop = Utils.loadConf(Utils.pathToConf)
    }

    /**
     * Load files to perform filter transformation and gather data to store
     */
    val logger = LoggerFactory.getLogger(Launcher.getClass)

    val prop = Utils.prop
    var lkpRepetitif = Utils.sqlContext.emptyDataFrame
    var ticketEnrich = Utils.sqlContext.emptyDataFrame
    val fosav = IncidentologieFosav.loadFile()
    val wasac = IncidentologieWasac.loadFile()
    val loadInterimFosavWasac = IncidentologieFosav.loadInterim()
    val acort = Acort.loadFile()

    val wasacIai = IncidentologieWasacIai.loadFile()
    val communes = CommunesInseeUtile.loadFile()
    val correspondence = CorrespondanceCodeInseecodePostalUtile.loadFile()

    val allRapReference = AllRapReference.loadFile()
    val categoriesIncidents = CategoriesIncidents.loadFile()
    val nrgtrreferentiel = NRGTRReferentiel.loadFile()

    lkpRepetitif = LKPRepetitif.lookReplicate()

    val lkpActivations = LKPActivations.loadFile()
    val soipadIndicOceane = SoipadIndicOceane.loadFile()

    /**
     * Begin transformation flow by join with loaded files
     */
    val wasacIaiIosTrans = IncidentologieWasacIai.transformIos(wasacIai)

    val wasacIaiNpPairesTrans = IncidentologieWasacIai.transformNbPaires(wasacIai)

    val fosavTrans = IncidentologieFosav.transform(fosav)
    val wasacTrans = IncidentologieWasac.replaceWasac(wasac)
    val joinFosavWasac = this.joinFosavWasac(fosavTrans, wasacTrans)
    Store.storeFosavWasacFilter(joinFosavWasac)

    val sortedAcort = FilterEnrichI.sortAcort(acort)
    val joinFosavAcort = this.joinFosavAcort(loadInterimFosavWasac, sortedAcort)

    val acortTrans = FilterEnrichI.transformAcort(joinFosavAcort, acort, sortedAcort)
    Store.storeFilter(acortTrans)

    val communesTransform = CommunesInseeUtile.transform(communes)
    val correspondenceTransform = CorrespondanceCodeInseecodePostalUtile.transformCorrespondence(correspondence)
    val insee = this.joinCommunesCorrespondence(communesTransform, correspondence)
    Store.storeInsee(insee)

    val loadIntermediateAcort = Acort.loadInterimFile()

    val joinFosavAcortRapRef = this.joinFosavAcortRapRef(loadIntermediateAcort, allRapReference)

    val AllRapReferenceTransform = TransformEnrichI.transform(joinFosavAcortRapRef)
    val inseeInterim = CommunesInseeUtile.loadInterim()

    val gatherInsee = this.joinWithInsee(AllRapReferenceTransform, inseeInterim)

    val gatherCategories = GatherEnrichI.transform(gatherInsee, categoriesIncidents)

    val gatherNrGTRReferentiel = this.joinWithNRGTRReferentiel(gatherCategories, nrgtrreferentiel)

    val renameColWasacIaiIos = IncidentologieWasacIai.renameColumnWasacIaiIos(wasacIaiIosTrans)

    val gatherWasacIaiIos = this.joinWithWasacIaiIos(gatherNrGTRReferentiel, renameColWasacIaiIos)

    val gatherWasacIaiNbPaires = this.joinWithWasacIaiNbPaires(gatherWasacIaiIos, wasacIaiNpPairesTrans)

    val joinLKPRepetitif = this.joinWithRepetitif(gatherWasacIaiNbPaires, lkpRepetitif)

    val gatherLKPRepetitif = LKPRepetitif.transform(joinLKPRepetitif)
    val joinLKPActivations = LKPActivations.transform(lkpActivations)
    val gatherLKPActivations = this.joinWithLKPActivations(gatherLKPRepetitif, joinLKPActivations)

    val joinSoipadIndicOceane = this.joinWithSoipadIndicOceane(gatherLKPActivations, soipadIndicOceane)
    val gatherData = SoipadIndicOceane.transform(joinSoipadIndicOceane)

    //  if (prop.getString("dev.initialRun").equals("TRUE")) {
    //   ticketEnrich = gatherData
    // } else {
    val prevDayLoadFile = LKPRepetitif.interMediateloadFile()
    ticketEnrich = this.joinWithPrevDayEnrCurrEnrichI(gatherData, prevDayLoadFile)
    //  }

    /**
     * Write the data to hive location
     */
    Store.storeHive(ticketEnrich)
    Utils.closeContext()

  }

  /**
   * Joining the file incidentologie_FOSAV and incidentologie_WASAC isong left outer join with key "feuillet"
   */
  def joinFosavWasac(fosav: DataFrame, wasac: DataFrame): DataFrame = {

    val joinedFOSAV = fosav.as('a).join(wasac.as('b), $"a.feuillet" === $"b.feuillet", "left_outer")
    val selectFOSAVWASAC = joinedFOSAV.select($"a.*", $"b.ip_admin", $"b.type_routeur", $"b.ios", $"b.constructeur", $"b.chassis")
    return selectFOSAVWASAC
  }

  /**
   * Fosav and acort join
   * Left join on feuillet and element_reseau
   */
  def joinFosavAcort(fosav: DataFrame, acort: DataFrame): DataFrame = {
    val acortFosavJoin = fosav.as('a).join(acort.as('b), fosav("feuillet") === acort("element_reseau"), "left_outer")
    val acortFosavSelectPart = acortFosavJoin.select($"a.*", $"b.num_pivot", $"b.element_reseau", $"b.type_interne", $"b.role", $"b.type_si")
    val acortFosavSelect = acortFosavSelectPart.withColumn("date_retab_ticket", $"date_retab_ticket".cast("string"))
      .withColumn("date_debut_ticket", $"date_debut_ticket".cast("string"))
      .withColumn("date_cloture_ticket", $"date_cloture_ticket".cast("string"))
      .withColumn("duree_gel", $"duree_gel".cast("string"))
      .withColumn("dt_deb_suivi", $"dt_deb_suivi".cast("string"))
      .withColumn("date_creation_ticket", $"date_creation_ticket".cast("string"))
      .withColumn("datemescom", $"datemescom".cast("string"))
      .withColumn("dt_fin_suivi", $"dt_fin_suivi".cast("string"))
      .withColumn("dt_deb_pec", $"dt_deb_pec".cast("string"))
      .withColumn("dt_deb_cause", $"dt_deb_cause".cast("string"))
      .withColumn("dt_fin_cause", $"dt_fin_cause".cast("string"))

    return acortFosavSelect
  }

  /**
   * Join the communes and correspondence file
   */
  def joinCommunesCorrespondence(communes: DataFrame, correspondence: DataFrame): DataFrame = {

    val joinInsee = communes.as('a).join(correspondence.as('b), $"a.code_insee" === $"b.Code_INSEE", "inner")
    val insee = joinInsee.select($"a.nom_commune", $"a.poulation_tot", $"a.code_insee", $"a.nom_commune_caps", $"a.nom_commune_caps2", $"b.Code_Postal")
    return insee
  }

  /**
   * Join the output of Fosav_Acort_filter file with AllRapRefrence file and select the required columns
   */
  def joinFosavAcortRapRef(fosavAcort: DataFrame, allRapReference: DataFrame): DataFrame = {

    val joinedFosavAcortAllRapRef = fosavAcort.as('a).join(allRapReference.as('b), $"a.rap" === $"b.rap", "left_outer")

    val selectedjoinFosavAcortAllRapRef = joinedFosavAcortAllRapRef.select($"a.num_ticket", $"a.identifiant_eds_pilote", $"a.poste_utilisateur", $"a.origine", $"a.date_creation_ticket", $"a.date_debut_ticket", $"a.date_cloture_ticket", $"a.date_retablissement", $"a.sem_cloture_ticket", $"a.mois_cloture_ticket", $"a.mois_cloture_ticket_indus", $"a.raison_sociale",
      $"a.raison_sociale_ticket", $"a.addresse_complete_client", $"a.code_postal_client", $"a.siren", $"a.datemescom", $"a.type_produit", $"a.etat_produit", $"a.description",
      $"a.nature_finale", $"a.responsabilite_pbm", $"a.famille_de_probleme", $"a.detail_probleme", $"a.gtr", $"a.plage_horaire_gtr", $"a.societe_extremite_A",
      $"a.ville_extremite_A", $"a.voie_extremite_A", $"a.cp_extremite_A", $"a.societe_extremite_B", $"a.ville_extremite_B", $"a.voie_extremite_B", $"a.cp_extremite_B",
      $"a.identifiant_2_produit", $"a.identifiant_3_produit", $"a.identifiant_4_produit", $"a.gtr_respecte", $"a.is_repetitif", $"a.complement_interne",
      $"a.libelle_imputation", $"a.libelle_succint", $"a.type_ticket", $"a.donnee_complementaire", $"a.initiateur_nom_utilisateur", $"a.duree_gel",
      $"a.duree_constractuelle_indispo", $"a.nature_initiale", $"a.nombre_der_clos_produit_3_mois", $"a.dependance_ticket", $"a.imputation_princ", $"a.eds_active",
      $"a.poste_active", $"a.dt_deb_suivi", $"a.dt_fin_suivi", $"a.dt_deb_pec", $"a.duree_totale", $"a.pht_idtcom", $"a.ip_admin", $"a.ios_version", $"a.constructeur",
      $"a.chassis", $"a.num_pivot", $"a.element_reseau", $"a.type_interne", $"a.duree_indisponibilite", $"a.duree_indisponibilite_hors_gel", $"a.delai_activation_pec",
      $"a.identifiant_1_produit", $"a.is_gtrisable", $"a.ce_id", $"a.connexion_id", $"a.support_id", $"a.identifiant_sous_reseau", $"a.nb_paires", $"a.collect_role",
      $"a.router_role", $"a.bas_id", $"a.category", $"a.subcategory", $"a.cause_nrgtr", $"a.responsabilite_nrgtr", $"a.ios_version_from_iai", $"a.dslam_id",
      $"a.master_dslam_id", $"a.nortel_id", $"a.pe_id", $"a.fav_id", $"a.ntu_id", $"a.tronc_type", $"a.population", $"a.population_cat", $"a.rap",
      $"b.support_type_nominal", $"b.support_nominal", $"b.support_type_secours", $"b.support_secours", $"b.collecte_nominal", $"b.collecte_secours")
    return selectedjoinFosavAcortAllRapRef
  }

  /**
   * Join the merged data with insee intermediate file and get population from insee
   */

  def joinWithInsee(dataFrame: DataFrame, insee: DataFrame): DataFrame = {
    val joinedInsee = dataFrame.as('a).join(insee.as('b), $"a.cp_extremite_A" === $"b.code_postal" && $"a.ville_extremite_A" === $"b.nom_commune_caps2", "left_outer")
    val getPopulation = joinedInsee.drop("population").select($"a.*", $"b.poulation_tot").withColumnRenamed("poulation_tot", "population")
    val getInsee = getPopulation.drop("category").drop("subcategory").select("*")
    return getInsee

  }
  /**
   * Join the above transformation output with NRGTRreferentiel
   */
  def joinWithNRGTRReferentiel(dataFrame: DataFrame, nrgtrReferentiel: DataFrame): DataFrame = {

    val joinNRGTRreferentiel = dataFrame.as('a).join(nrgtrReferentiel.as('b), $"a.complement_interne" === $"b.complement_interne", "left_outer").select($"a.*", $"b.cause_nrgtr", $"b.responsabilite_nrgtr").drop("ios_version_from_iai")
    return joinNRGTRreferentiel
  }
  /**
   * Read the incidentologie_wasac_iai  and join with previous merged data
   */
  def joinWithWasacIaiIos(dataFrame: DataFrame, wasacIaiIos: DataFrame): DataFrame = {
    val gatherWasacIaiIos = dataFrame.as('a).join(wasacIaiIos.as('b), $"a.num_ticket" === $"b.ticketid" && $"a.pht_idtcom" === $"b.feuillet", "left_outer").select($"a.*", $"b.ios_version_from_iai").drop("nb_paires")
    return gatherWasacIaiIos
  }

  def joinWithWasacIaiNbPaires(dataFrame: DataFrame, wasacIaiNbPaires: DataFrame): DataFrame = {
    val gatherWasacIaiNbPaires = dataFrame.as('a).join(wasacIaiNbPaires.as('b), $"a.num_ticket" === $"b.ticketid" && $"a.pht_idtcom" === $"b.feuillet", "left_outer").select($"a.*", $"b.nb_paires").drop("is_repetitif").drop("eds_active")
    return gatherWasacIaiNbPaires
  }
  /**
   * Read the lookup Repetitif  file to perform the transformation
   */
  def joinWithRepetitif(dataFrame: DataFrame, lkpRepetitif: DataFrame): DataFrame = {
    val windowPartition = Window.partitionBy($"ce_id", $"category", $"subcategory")
    val lkpRepetitifRank = lkpRepetitif.select($"*", rowNumber.over(windowPartition).alias("rn"))

    val joinLKPRepetitif = dataFrame.as('a).join(lkpRepetitifRank.as('b), $"a.ce_id" === $"b.ce_id" && $"a.category" === $"b.category" && $"a.subcategory" === $"b.subcategory", "left_outer").select($"a.*", $"b.nb_repetitions")

    return joinLKPRepetitif
  }
  /**
   * Join with Lookup Activations File
   */
  def joinWithLKPActivations(dataFrame: DataFrame, lkpActivations: DataFrame): DataFrame = {
    val lkpAct = dataFrame.as('a).join(lkpActivations.as('b), $"a.num_ticket" === $"b.pht_idttic", "left_outer")
    val joinWithLKPActivations = lkpAct.select($"a.*", $"b.*").withColumn("eds_active_1", $"eds1").withColumn("eds_active_2", $"eds2").withColumn("eds_active_3", $"eds3").withColumn("eds_active_4", $"eds4").withColumn("eds_active_5", $"eds5")
    return joinWithLKPActivations
  }

  /**
   * Read the file soipad_indic_oceane and perform join
   */
  def joinWithSoipadIndicOceane(dataFrame: DataFrame, soipadIndicOceane: DataFrame): DataFrame = {
    val joinSoipadIndicOceane = dataFrame.as('a).join(soipadIndicOceane.as('b), $"a.num_ticket" === $"b.pht_idttic", "left_outer").select($"a.*", $"b.tic_dg_auto") //.distinct()
    return joinSoipadIndicOceane
  }

  /**
   * Get only the records from previous day which is not present in current data and union with current day record
   */
  def joinWithPrevDayEnrCurrEnrichI(gatherData: DataFrame, prevDayLoadFile: DataFrame): DataFrame = {
    val prevDayData = prevDayLoadFile.as('a).join(gatherData.as('b), $"a.num_ticket" === $"b.num_ticket", "left_outer").where($"b.num_ticket".isNull).select($"a.*")
    val intermediateResult = prevDayData.unionAll(gatherData)
    val result = intermediateResult //.distinct()
    return result
  }
}
