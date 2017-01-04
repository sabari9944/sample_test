package com.obs.pn.ticketenrichi.transf

import java.io.FileNotFoundException
import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rowNumber
import org.slf4j.LoggerFactory
import com.obs.pn.acq.Acquisition
import com.obs.pn.ticketenrichi.util.CommunesInseeUtile
import com.obs.pn.ticketenrichi.util.CorrespondanceCodeInseecodePostalUtile
import com.obs.pn.ticketenrichi.acq.incidentologiefosav.IncidentologieFosav
import com.obs.pn.acq.incidentologiewasaciai.IncidentologieWasacIai
import com.obs.pn.ticketenrichi.acq.lkpactivations.LKPActivations
import com.obs.pn.ticketenrichi.acq.lkprepetitif.LKPRepetitif
import com.obs.pn.commons.Utils
import com.obs.pn.commons.Utils.sqlContext.implicits.StringToColumn
import com.obs.pn.commons.Utils.sqlContext.implicits.localSeqToDataFrameHolder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

object Launcher {

  /** logger */
  val logger = LoggerFactory.getLogger(Launcher.getClass)

  /**
   * Launcher is the driver object to drive flow of Enrichi
   * Enrich flow is divided basically in to Filter,Transform and gather parts
   */

  def main(args: Array[String]): Unit = {
    /**
     * Load files to perform filter transformation and gather data to store
     */
    logger.info(" STARTING TICKET ENRICHI MAIN CLASS ")

    /**
     *  Read data parameter from command line
     */
    try {
      if (args.length < 2) {
        /**
         * System validation for date parameter, this is system err not println
         */
        System.err.println("Launch Parameter Required:  <Date:yyyymmdd> <HDFSPATH: Config File> <Date:yyyymmdd>")
        System.exit(1)
      }

      System.setProperty("obspn.application.conf", args(1))
      System.setProperty("obspn.application.master", "")
      System.setProperty("obspn.application.appname", "ticketenrichi")

      logger.debug(" RUN DATE " + args(0))
      Utils.runDate = args(0)

      //launching process
      ticketenrichi()

    } catch {
      case e: Exception => {
        logger.error("FATAL ERROR on ticket enrichi", e)
        /**
         * Zero => Everything Okay
         * Positive => Something I expected could potentially go wrong went wrong (bad command-line, can't find file, could not connect to server)
         * Negative => Something I didn't expect at all went wrong (system error - unanticipated exception - externally forced termination e.g. kill -9)
         */
        System.exit(-1)
      }
    } finally {
      logger.info(" CLOSE CONTEXT")
      Utils.sc.stop()
    }
  }

  def ticketenrichi() = {
    logger.info("*** Start File loadings ***")

    val fosav = Acquisition.loadTimestampFile("dev.incidentologie_FOSAV", Utils.runDate, false)
    val wasac = Acquisition.loadTimestampFile("dev.incidentologie_wasac", Utils.runDate, false)
    val acort = Acquisition.loadTimestampFile("dev.acort", Utils.runDate, false)
    val wasacIai = Acquisition.loadTimestampFile("dev.incidentologie_wasac_iai", Utils.runDate, false)
    val communes = Acquisition.loadFile("dev.communes_INSEE_utile", false)
    val correspondence = Acquisition.loadFile("dev.correspondance-code-insee-code-postal_utile", false)
    val allRapReference = Acquisition.loadFile("dev.AllRapReference", false)
    val categoriesIncidents = Acquisition.loadFile("dev.CategoriesIncidents", false)
    val nrgtrreferentiel = Acquisition.loadFile("dev.NRGTRreferentiel", false)
    val lkpRepetitif = LKPRepetitif.lookReplicate()
    val lkpActivations = Acquisition.loadTimestampFile("dev.LKP_ACTIVATIONS", Utils.runDate, false)
    val soipadIndicOceane = Acquisition.loadTimestampFile("dev.soipad_indic_oceane", Utils.runDate, false)

    logger.info("*** loadings ARE DONE ***")

    /**
     * Begin transformation flow by join with loaded files
     */
    logger.info("*** STARTING TRANSFORMATIONS ***")
    val wasacIaiIosTrans = IncidentologieWasacIai.transformIos(wasacIai)
    logger.info("*** Completed wasacIaiIosTrans ***")
    val wasacIaiNpPairesTrans = IncidentologieWasacIai.transformNbPaires(wasacIai)
    logger.info("*** Completed wasacIaiNpPairesTrans ***")
    val fosavTrans = IncidentologieFosav.transform(fosav)
    logger.info("*** Completed fosavTrans ***")
    val wasacTrans = TransformEnrichI.replaceWasac(wasac)
    logger.info("*** Completed wasacTrans ***")
    val joinFosavWasac = this.joinFosavWasac(fosavTrans, wasacTrans).persist()
    logger.info("*** Completed joinFosavWasac ***")
    val sortedAcort = FilterEnrichI.sortAcort(acort)
    logger.info("*** Completed sortedAcort ***")
    val joinFosavAcort = this.joinFosavAcort(joinFosavWasac, sortedAcort)
    joinFosavWasac.unpersist()
    logger.info("*** Completed joinFosavAcort ***")
    val acortTrans = FilterEnrichI.transformAcort(joinFosavAcort, acort, sortedAcort).persist()
    logger.info("*** Completed acortTrans ***")
    val communesTransform = CommunesInseeUtile.transform(communes)
    logger.info("*** Completed communesTransform ***")
    val correspondenceTransform = CorrespondanceCodeInseecodePostalUtile.transformCorrespondence(correspondence)
    logger.info("*** Completed correspondenceTransform ***")
    val insee = this.joinCommunesCorrespondence(communesTransform, correspondence)
    logger.info("*** Completed insee ***")
    val joinFosavAcortRapRef = this.joinFosavAcortRapRef(acortTrans, allRapReference)
    acortTrans.unpersist()
    logger.info("*** Completed joinFosavAcortRapRef ***")
    val AllRapReferenceTransform = TransformEnrichI.transform(joinFosavAcortRapRef)
    logger.info("*** Completed AllRapReferenceTransform ***")
    val gatherInsee = this.joinWithInsee(AllRapReferenceTransform, insee)
    logger.info("*** Completed gatherInsee ***")
    val gatherCategories = GatherEnrichI.transform(gatherInsee, categoriesIncidents)
    logger.info("*** Completed gatherCategories ***")
    val gatherNrGTRReferentiel = this.joinWithNRGTRReferentiel(gatherCategories, nrgtrreferentiel)
    logger.info("*** Completed gatherNrGTRReferentiel ***")
    val renameColWasacIaiIos = IncidentologieWasacIai.renameColumnWasacIaiIos(wasacIaiIosTrans)
    logger.info("*** Completed renameColWasacIaiIos ***")
    val gatherWasacIaiIos = this.joinWithWasacIaiIos(gatherNrGTRReferentiel, renameColWasacIaiIos)
    logger.info("*** Completed renameColWasacIaiIos ***")
    val gatherWasacIaiNbPaires = this.joinWithWasacIaiNbPaires(gatherWasacIaiIos, wasacIaiNpPairesTrans)
    logger.info("*** Completed gatherWasacIaiNbPaires ***")
    val joinLKPRepetitif = this.joinWithRepetitif(gatherWasacIaiNbPaires, lkpRepetitif)
    logger.info("*** Completed joinLKPRepetitif ***")
    val gatherLKPRepetitif = LKPRepetitif.transform(joinLKPRepetitif)
    logger.info("*** Completed gatherLKPRepetitif ***")
    val joinLKPActivations = LKPActivations.transform(lkpActivations)
    logger.info("*** Completed joinLKPActivations ***")
    val gatherLKPActivations = this.joinWithLKPActivations(gatherLKPRepetitif, joinLKPActivations)
    logger.info("*** Completed gatherLKPActivations ***")
    val joinSoipadIndicOceane = this.joinWithSoipadIndicOceane(gatherLKPActivations, soipadIndicOceane)
    logger.info("*** Completed joinSoipadIndicOceane ***")
    val prevDayLoadFile = LKPRepetitif.interMediateloadFile()
    logger.info(" JOIN LKP REPETITIF")
    val ticketEnrich = this.joinWithPrevDayEnrCurrEnrichI(joinSoipadIndicOceane, prevDayLoadFile)
    logger.info("*** Completed ticketEnrich ***")

    /**
     * Write the data to hive location
     */
    logger.info(" STORE RESULT TO HIVE")
    Store.storeHive(ticketEnrich)
    logger.info(" CLOSE CONTEXT")
    Utils.sc.stop()
  }

  /**
   * Joining the file incidentologie_FOSAV and incidentologie_WASAC.
   * Left outer join on key "feuillet"
   * @param fosav DataFrame generated from incidentologie_FOSAV
   * @param wasac DataFrame generated from incidentologie_WASAC
   * @return joinedFOSAV
   * @throws Exception if the join with Fosav wasac doesn't works
   */
  def joinFosavWasac(fosav: DataFrame, wasac: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinFosavWasac (FOSAV & WASAC) ")
      val joinedFOSAV = fosav.as('a).join(wasac.as('b), $"a.feuillet" === $"b.feuillet", "left_outer")
      joinedFOSAV.select($"a.*", $"b.ip_admin", $"b.type_routeur", $"b.ios", $"b.constructeur", $"b.chassis")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinFosavWasac FOSAV & WASAC")
        throw e
      }
    }
  }

  /**
   * Joining the FosavWasac Output with and Acort File.
   * Left join on feuillet and element_reseau
   * @param fosav DataFrame from fosavWasac Result
   * @param acort sorted Acort
   * @return acortFosavSelectPart
   * @throws Exception if the join with Fosav Acort doesn't works
   */
  def joinFosavAcort(fosav: DataFrame, acort: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinFosavAcort (FOSAV & WASAC) AND (ACORT)")
      val acortFosavJoin = fosav.as('a).join(acort.as('b), fosav("feuillet") === acort("element_reseau"), "left_outer")
      val acortFosavSelectPart = acortFosavJoin.select($"a.*", $"b.num_pivot", $"b.element_reseau", $"b.type_interne", $"b.role", $"b.type_si")
      acortFosavSelectPart.withColumn("date_retab_ticket", $"date_retab_ticket".cast("string"))
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
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinFosavAcort (FOSAV & WASAC) AND (ACORT)")
        throw e
      }
    }

  }

  /**
   * Join the communesInseeUtile and correspondancecodeInseeUtile file.
   * Inner Join on code_insee
   * @param communes DataFrame from communesInseeUtile
   * @return Dataframe Joins of the Communes and Correspondance INSEE
   * @throws Exception if the join with CommunesCorrespondence doesn't works
   */
  def joinCommunesCorrespondence(communes: DataFrame, correspondence: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinCommunesCorrespondence INSEE (communes) AND (correspondence)")
      val joinInsee = communes.as('a).join(correspondence.as('b), $"a.code_insee" === $"b.Code_INSEE", "inner")
      joinInsee.select($"a.nom_commune", $"a.poulation_tot", $"a.code_insee", $"a.nom_commune_caps", $"a.nom_commune_caps2", $"b.Code_Postal")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinCommunesCorrespondence INSEE (communes) AND (correspondence)")
        throw e
      }
    }
  }

  /**
   * Join the output of FosavWasacAcortfilter Result with AllRapRefrence file
   * left_outer join on rap
   * @param fosavAcort FosavWasacAcortfilter Result
   * @param allRapReference AllRapreference File
   * @return joinedFosavAcortAllRapRef
   * @throws Exception if the join with FosavAcortRapRef doesn't works
   */
  def joinFosavAcortRapRef(fosavAcort: DataFrame, allRapReference: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinFosavAcortRapRef (FOSAV & WASAC & ACORT) JOIN (ALLRAP)")
      val joinedFosavAcortAllRapRef = fosavAcort.as('a).join(allRapReference.as('b), $"a.rap" === $"b.rap", "left_outer")

      joinedFosavAcortAllRapRef.select($"a.num_ticket", $"a.identifiant_eds_pilote", $"a.poste_utilisateur", $"a.origine", $"a.date_creation_ticket", $"a.date_debut_ticket", $"a.date_cloture_ticket", $"a.date_retablissement", $"a.sem_cloture_ticket", $"a.mois_cloture_ticket", $"a.mois_cloture_ticket_indus", $"a.raison_sociale",
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

    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinFosavAcortRapRef (FOSAV & WASAC & ACORT) JOIN (ALLRAP)")
        throw e
      }
    }

  }

  /**
   * Join the Transformed Output with Communes and Correspondance Transformed File
   * left join on cp_extremite_A with code_postal and ville_extremite_A with nom_commune_caps2
   * @param dataFrame Transformed DataFrame
   * @param insee Communes and Correspondance Transformed File
   * @return getPopulation
   * @throws Exception if the join with Insee doesn't works
   */

  def joinWithInsee(dataFrame: DataFrame, insee: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithInsee (FOSAV & WASAC & ACORT & ALLRAP) JOIN (INSEE)")
      val joinedInsee = dataFrame.as('a).join(insee.as('b), $"a.cp_extremite_A" === $"b.code_postal" && $"a.ville_extremite_A" === $"b.nom_commune_caps2", "left_outer")
      val getPopulation = joinedInsee.drop("population").select($"a.*", $"b.poulation_tot").withColumnRenamed("poulation_tot", "population")
      getPopulation.drop("category").drop("subcategory").select("*")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithInsee (FOSAV & WASAC & ACORT & ALLRAP) JOIN (INSEE)")
        throw e
      }
    }
  }

  /**
   * Join the Transformed DataFrame with NRGTRreferentiel
   * left join on complement_interne
   * @param dataFrame Transformed DataFrame
   * @param nrgtrReferentiel NRGTRreferentiel File
   * @return dataFrame
   * @throws Exception if the join with Referentiel doesn't works
   */
  def joinWithNRGTRReferentiel(dataFrame: DataFrame, nrgtrReferentiel: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithNRGTRReferentiel (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents) JOIN (NRGTRreferentie)")
      dataFrame.as('a).join(nrgtrReferentiel.as('b), $"a.complement_interne" === $"b.complement_interne", "left_outer").select($"a.*", $"b.cause_nrgtr", $"b.responsabilite_nrgtr").drop("ios_version_from_iai")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithNRGTRReferentiel (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents) JOIN (NRGTRreferentie)")
        throw e
      }
    }
  }

  /**
   * Join the Transformed DataFrame with Incidentologie_wasac_iai transformed data for ios
   * @param dataFrame Transformed DataFrame
   * @param wasacIaiIos DataFrame from transformation on incidentologie_wasac_iai
   * @throws Exception if the join with WasacIaiIos doesn't works
   */
  def joinWithWasacIaiIos(dataFrame: DataFrame, wasacIaiIos: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithWasacIaiIos (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie) JOIN (WASAC_IAI_IOS)")
      dataFrame.as('a).join(wasacIaiIos.as('b), $"a.num_ticket" === $"b.ticketid" && $"a.pht_idtcom" === $"b.feuillet", "left_outer").select($"a.*", $"b.ios_version_from_iai").drop("nb_paires")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithWasacIaiIos (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie) JOIN (WASAC_IAI_IOS)")
        throw e
      }
    }
  }

  /**
   * Join the Transformed DataFrame with Incidentologie_wasac_iai transformed data for nb_paires
   * @param dataFrame Transformed DataFrame
   * @param wasacIaiIos DataFrame from transformation on incidentologie_wasac_iai
   * @throws Exception if the join with WasacIaiNbPaires doesn't works
   */

  def joinWithWasacIaiNbPaires(dataFrame: DataFrame, wasacIaiNbPaires: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithWasacIaiNbPaires (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS) JOIN (WASAC_IAI_NP)")
      dataFrame.as('a).join(wasacIaiNbPaires.as('b), $"a.num_ticket" === $"b.ticketid" && $"a.pht_idtcom" === $"b.feuillet", "left_outer").select($"a.*", $"b.nb_paires").drop("is_repetitif").drop("eds_active")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithWasacIaiNbPaires (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS) JOIN (WASAC_IAI_NP)")
        throw e
      }
    }
  }

  /**
   * Join the transformed data with the LookUp repetitif file generated from history data
   * left join on ce_id, category and subcategory
   * @param dataFrame Transformed DataFrame
   * @param lkpRepetitif Lookup File
   * @return dataFrame
   * @throws Exception if the join with Repetitif doesn't works
   */
  def joinWithRepetitif(dataFrame: DataFrame, lkpRepetitif: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithRepetitif (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) LOOKUP JOIN (lkpRepetitif)")
      val windowPartition = Window.partitionBy($"ce_id", $"category", $"subcategory")
      val lkpRepetitifRank = lkpRepetitif.select($"*", rowNumber.over(windowPartition).alias("rn"))
      dataFrame.as('a).join(lkpRepetitifRank.as('b), $"a.ce_id" === $"b.ce_id" && $"a.category" === $"b.category" && $"a.subcategory" === $"b.subcategory", "left_outer").select($"a.*", $"b.nb_repetitions")
    } catch {
      case e: Exception => {
        logger.error("Unable to LOOKUP JOIN: joinWithRepetitif (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) LOOKUP JOIN (lkpRepetitif)")
        throw e
      }
    }
  }

  /**
   * Join transformed data with Lookup Activations File
   * left join on num_ticket with pht_idttic
   * @param dataFrame Transformed DataFrame
   * @param lkpActivations LookUp Activations File
   * @return lkpAct
   * @throws Exception if the join with LKPActivations doesn't works
   */
  def joinWithLKPActivations(dataFrame: DataFrame, lkpActivations: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithLKPActivations (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) LOOKUP JOIN (lkpActivations)")
      val lkpAct = dataFrame.as('a).join(lkpActivations.as('b), $"a.num_ticket" === $"b.pht_idttic", "left_outer")
      lkpAct.select($"a.*", $"b.*").withColumn("eds_active_1", $"eds1").withColumn("eds_active_2", $"eds2").withColumn("eds_active_3", $"eds3").withColumn("eds_active_4", $"eds4").withColumn("eds_active_5", $"eds5")
    } catch {
      case e: Exception => {
        logger.error("Unable to LOOKUP JOIN: joinWithLKPActivations (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) LOOKUP JOIN (lkpActivations)")
        throw e
      }
    }
  }

  /**
   * Join transformed data with SoipadIndicOceane File
   * left join on num_ticket with pht_idttic
   * @param dataFrame Transformed DataFrame
   * @param soipadIndicOceane SoipadIndicOceane File
   * @return soipadJoinDF
   * @throws Exception if the join with SoipadIndicOcean doesn't work
   */
  def joinWithSoipadIndicOceane(dataFrame: DataFrame, soipadIndicOceane: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithSoipadIndicOceane (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) JOIN (soipadIndicOceane)")
      val soipadJoinDF = dataFrame.as('a).join(soipadIndicOceane.as('b), $"a.num_ticket" === $"b.pht_idttic", "left_outer").select($"a.*", $"b.tic_dg_auto")
      soipadJoinDF.withColumnRenamed("libelle_succint", "libelle_succinct").select($"num_ticket", $"pht_idtcom", $"nombre_der_clos_produit_3_mois", $"is_repetitif", $"identifiant_1_produit", $"ce_id", $"connexion_id", $"support_id", $"identifiant_sous_reseau", $"service_support", $"collect", $"support", $"nb_paires", $"collect_role", $"type_produit", $"router_role", $"datemescom", $"etat_produit", $"identifiant_2_produit", $"identifiant_3_produit", $"identifiant_4_produit", $"identifiant_eds_pilote", $"poste_utilisateur", $"gtr", $"plage_horaire_gtr", $"gtr_respecte", $"origine", $"initiateur_nom_utilisateur", $"date_debut_ticket", $"date_creation_ticket", $"date_retablissement", $"date_cloture_ticket", $"duree_indisponibilite", $"duree_indisponibilite_hors_gel", $"duree_constractuelle_indispo", $"year_date", $"mois_cloture_ticket", $"mois_cloture_ticket_indus", $"sem_cloture_ticket", $"raison_sociale_ticket", $"raison_sociale", $"addresse_complete_client", $"code_postal_client", $"siren", $"societe_extremite_a", $"ville_extremite_a", $"voie_extremite_a", $"cp_extremite_a", $"societe_extremite_b", $"ville_extremite_b", $"voie_extremite_b", $"cp_extremite_b", $"bas_id", $"category", $"description", $"nature_initiale", $"nature_finale", $"responsabilite_pbm", $"famille_de_probleme", $"libelle_imputation", $"subcategory", $"detail_probleme", $"complement_interne", $"cause_nrgtr", $"responsabilite_nrgtr", $"donnee_complementaire", $"libelle_succinct", $"constructeur", $"chassis", $"ios_version", $"ios_version_from_iai", $"ip_admin", $"dslam_id", $"master_dslam_id", $"nortel_id", $"pe_id", $"fav_id", $"ntu_id", $"tronc_type", $"is_gtrisable", $"population", $"population_cat", $"rap", $"type_ticket", $"dependance_ticket", $"imputation_princ", $"eds_active_1", $"eds_active_2", $"eds_active_3", $"eds_active_4", $"eds_active_5", $"poste_active", $"dt_deb_suivi", $"dt_fin_suivi", $"dt_deb_pec", $"delai_activation_pec", $"duree_gel", $"duree_totale", $"tic_dg_auto", $"num_pivot", $"element_reseau", $"type_interne")
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithSoipadIndicOceane (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP) JOIN (soipadIndicOceane)")
        throw e
      }
    }
  }

  /**
   * Perform transformation toGet only the records from previous day
   * not present in current data and union with current day record.
   * @param gatherData Present data after transformation
   * @param prevDayLoadFile History data
   * @return prevDayData
   * @throws Exception if the join with the previous day ticket enrichis doesn't work
   */
  def joinWithPrevDayEnrCurrEnrichI(gatherData: DataFrame, prevDayLoadFile: DataFrame): DataFrame = {
    try {
      logger.debug("JOIN: joinWithPrevDayEnrCurrEnrichI (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP & SOIPAD) JOIN (HISTORY TICKET ENRICHI)")
      val prevDayLoadFileSelect = prevDayLoadFile.select($"num_ticket", $"pht_idtcom", $"nombre_der_clos_produit_3_mois", $"is_repetitif", $"identifiant_1_produit", $"ce_id", $"connexion_id", $"support_id", $"identifiant_sous_reseau", $"service_support", $"collect", $"support", $"nb_paires", $"collect_role", $"type_produit", $"router_role", $"datemescom", $"etat_produit", $"identifiant_2_produit", $"identifiant_3_produit", $"identifiant_4_produit", $"identifiant_eds_pilote", $"poste_utilisateur", $"gtr", $"plage_horaire_gtr", $"gtr_respecte", $"origine", $"initiateur_nom_utilisateur", $"date_debut_ticket", $"date_creation_ticket", $"date_retablissement", $"date_cloture_ticket", $"duree_indisponibilite", $"duree_indisponibilite_hors_gel", $"duree_constractuelle_indispo", $"year_date", $"mois_cloture_ticket", $"mois_cloture_ticket_indus", $"sem_cloture_ticket", $"raison_sociale_ticket", $"raison_sociale", $"addresse_complete_client", $"code_postal_client", $"siren", $"societe_extremite_a", $"ville_extremite_a", $"voie_extremite_a", $"cp_extremite_a", $"societe_extremite_b", $"ville_extremite_b", $"voie_extremite_b", $"cp_extremite_b", $"bas_id", $"category", $"description", $"nature_initiale", $"nature_finale", $"responsabilite_pbm", $"famille_de_probleme", $"libelle_imputation", $"subcategory", $"detail_probleme", $"complement_interne", $"cause_nrgtr", $"responsabilite_nrgtr", $"donnee_complementaire", $"libelle_succinct", $"constructeur", $"chassis", $"ios_version", $"ios_version_from_iai", $"ip_admin", $"dslam_id", $"master_dslam_id", $"nortel_id", $"pe_id", $"fav_id", $"ntu_id", $"tronc_type", $"is_gtrisable", $"population", $"population_cat", $"rap", $"type_ticket", $"dependance_ticket", $"imputation_princ", $"eds_active_1", $"eds_active_2", $"eds_active_3", $"eds_active_4", $"eds_active_5", $"poste_active", $"dt_deb_suivi", $"dt_fin_suivi", $"dt_deb_pec", $"delai_activation_pec", $"duree_gel", $"duree_totale", $"tic_dg_auto", $"num_pivot", $"element_reseau", $"type_interne")
      val prevDayData = prevDayLoadFileSelect.as('a).join(gatherData.as('b), $"a.num_ticket" === $"b.num_ticket", "left_outer").where($"b.num_ticket".isNull).select($"a.*")
      prevDayData.unionAll(gatherData)
    } catch {
      case e: Exception => {
        logger.error("Unable to JOIN: joinWithPrevDayEnrCurrEnrichI (FOSAV & WASAC & ACORT & ALLRAP & INSEE & CategoriesIncidents & NRGTRreferentie & WASAC_IAI_IOS & WASAC_IAI_NP& SOIPAD) JOIN (HISTORY TICKET ENRICHI))")
        throw e
      }
    }
  }

}
