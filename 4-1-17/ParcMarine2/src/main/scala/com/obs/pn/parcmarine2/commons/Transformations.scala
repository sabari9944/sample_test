package com.obs.pn.parcmarine2.commons

import parquet.org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import com.obs.pn.commons.Utils

/**
 * Helper class for SubGraph1. Transformation for SubGraph1 are declared in this Object.
 * Transformation file which does occur in the subgraph1.scala
 */
object Transformations {

  /** Logger instance */
  val logger = LoggerFactory.getLogger(Transformations.getClass)

  /** Get SQL context */
  val sqlContext = Utils.sqlContext

  /** Get configurations */
  val prop = Utils.prop

  import sqlContext.implicits._

  /**
   * Fields from fileBrHpr are selected with changed field names.
   * @param fileBrHpr DataFrame
   * @return DataFrame
   */
  def selectFieldsFromBrHpr(fileBrHpr: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromBrHpr started")
      /**
       * Code for cleansing the data.
       * In the BrHpr file, field ipr_datdebimgprd  casted as date.
       * If the field is empty then putting 'null' value in the field.
       *  Otherwise (if field has some values) then setting the value for the field.
       */
      val res = fileBrHpr.selectExpr("cast(ipr_datdebimgprd as date) as ipr_datdebimgprd",
        "cast(ipr_datfinimgprd as date) as ipr_datfinimgprd", "isr_idtsoures", "ipr_idtetaprd",
        "ipr_idtcom", "cast(ipr_idtprd as string) as ipr_idtprd", "ipr_lbtypprd")
        .withColumn("ipr_lbtypprd", when($"ipr_lbtypprd" === "", null)
          .otherwise($"ipr_lbtypprd")).withColumn("ipr_idtetaprd",
          when($"ipr_idtetaprd" === "", null).otherwise($"ipr_idtetaprd"))

      logger.info("selectFieldsFromBrHpr completed successfully")
      res
    }

  /**
   * Selecting only required fields from fileBrTie and skipping unwanted fields.
   * @param fileBrHpr dataframe
   * @return dataFrame
   */
  def selectFieldsFromBrTie(fileBrTie: DataFrame): DataFrame =
    {

      logger.debug("selectFieldsFromBrTie started")

      val res = fileBrTie.selectExpr("tie_idttie", "tie_raiscltie",
        "tie_sirtie", "tie_voibptie", "tie_cmpvoitie", "tie_codptlcomtie", "tie_comtie", "tie_lbpaytie")

      logger.info("selectFieldsFromBrTie completed successfully")
      res
    }

  /**
   * Filtering out required fields from join (fileBrEds and fileBrIsr) output.
   * @param join1
   * @return DataFrame
   */
  def selectFieldsFromBrEdsJoinBrIsr(join1: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromBrEdsJoinBrIsr started")
      /**
       * Code for cleansing the data.
       * Checking fields isr_idtsoures and tie_idttie for blank values.
       * If the field is empty then putting 'null' value in the field.
       *  Otherwise (if field has some values) then setting the value for the field.
       */
      val res = join1.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"b.isr_idtsoures", $"b.isr_datdebimgsoures", $"b.isr_datfinimgsoures", $"b.tie_idttie")
        .withColumn("isr_idtsoures", when($"b.isr_idtsoures" === "", null)
          .otherwise($"b.isr_idtsoures"))
        .withColumn("tie_idttie", when($"b.tie_idttie" === "", null)
          .otherwise($"b.tie_idttie"))
      logger.info("selectFieldsFromBrEdsJoinBrIsr completed successfully")
      res
    }

  /**
   * Filtering out required fields from join (BrIpr2 and BrHpr) output.
   * @param join2 dataFrame
   * @return dataFrame
   */
  def selectFieldsFromBrIpr2joinBrHpr(join2: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromBrIpr2joinBrHpr started ")
      /**
       * First filtering out required fields from join dataframe.
       * Then filtering out records from filtered dataframe if ipr_idtetaprd field is not empty
       * and if ipr_idtetaprd field has value in (PARC PARTIEL,PARC TOTAL,CARNET, EN COURS).
       */
      val res = join2.select($"b.ipr_datdebimgprd", $"b.ipr_datfinimgprd", $"b.isr_idtsoures",
        $"b.ipr_idtetaprd", $"b.ipr_idtcom", $"b.ipr_idtprd", $"b.ipr_lbtypprd", $"a.ipr_voibpexta",
        $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta",
        $"a.ipr_voibpextb", $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb",
        $"a.ipr_steutlextb", $"a.ipr_datmescom")
        .filter($"ipr_idtetaprd" === "PARC PARTIEL" || $"ipr_idtetaprd" === "PARC TOTAL"
          || $"ipr_idtetaprd" === "CARNET" || $"ipr_idtetaprd" === "EN COURS")
        .filter($"ipr_lbtypprd" !== "")

      logger.info("selectFieldsFromBrIpr2joinBrHpr completed successfully")
      res
    }

  /**
   * Filtering out required fields from join (join2 and BrGar) output.
   * @param join3 dataFrame
   * @return dataFrame
   */
  def selectFieldsFromBrGarJoin(join3: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromBrGarJoin started ")
      /**
       * The join operation by default adds all the columns from both Left side realtion and
       * right side relation into the join output dataframe. This results in many unwanted columns
       * present in join output dataframe.
       * Following code is selecting only required fields from left relation (alias 'a')
       * and right relation (alias 'b') for the join4 output dataframe.
       */
      val res = join3.select($"a.ipr_datdebimgprd", $"a.ipr_datfinimgprd", $"a.isr_idtsoures",
        $"a.ipr_idtetaprd", $"a.ipr_idtcom", $"a.ipr_idtprd", $"a.ipr_lbtypprd", $"a.ipr_voibpexta",
        $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta", $"a.ipr_voibpextb",
        $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb", $"a.ipr_steutlextb", $"a.ipr_datmescom",
        $"b.gar_lbcodgar", $"b.gar_lbplghor")

      logger.info("selectFieldsFromBrGarJoin completed successfully")
      res
    }

  /**
   * Filtering out required fields from join (output of join1 and output of join3) .
   * @param join4 dataFrame
   * @return dataFrame
   */
  def selectFieldsFromBrEdsBrIsrJoinBrGar(join4: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromBrEdsBrIsrJoinBrGar started ")
      /**
       * The join operation by default adds all the columns from both Left side realtion and
       * right side relation into the join output dataframe. This results in many unwanted columns
       * present in join output dataframe.
       * Following code is selecting only required fields from left relation (alias 'a')
       * and right relation (alias 'b') for the join4 output dataframe.
       */
      val res = join4.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"a.isr_idtsoures",
        $"a.isr_datdebimgsoures", $"a.isr_datfinimgsoures", $"a.tie_idttie", $"b.ipr_datdebimgprd",
        $"b.ipr_datfinimgprd", $"b.ipr_idtetaprd", $"b.ipr_idtcom", $"b.ipr_lbtypprd", $"b.ipr_voibpexta",
        $"b.ipr_comexta", $"b.ipr_codptlcomexta", $"b.ipr_lbpayexta", $"b.ipr_steutlexta", $"b.ipr_voibpextb",
        $"b.ipr_comextb", $"b.ipr_codptlcomextb", $"b.ipr_lbpayextb", $"b.ipr_steutlextb", $"b.ipr_datmescom",
        $"b.gar_lbcodgar", $"b.gar_lbplghor")

      logger.info("selectFieldsFromBrEdsBrIsrJoinBrGar completed successfully")
      res
    }

  /**
   * Filtering out required fields from join (output of join4 and BrTie) output.
   * @param join5 dataFrame
   * @return dataFrame
   */
  def selectFieldsFromAllJoionsFinalResultset(join5: DataFrame): DataFrame =
    {
      logger.debug("selectFieldsFromAllJoionsFinalResultset started")
      /**
       * The join operation by default adds all the columns from both Left side realtion and
       * right side relation into the join output dataframe. This results in many unwanted columns
       * present in join output dataframe.
       * Following code is selecting only required fields from left relation (alias 'a')
       * and right relation (alias 'b') for the join4 output dataframe.
       */
      val res = join5.select($"a.eds_idteds", $"a.eds_lbapteds", $"a.eds_nomcrteds", $"a.isr_idtsoures",
        $"a.isr_datdebimgsoures", $"a.isr_datfinimgsoures", $"a.tie_idttie", $"a.ipr_datdebimgprd",
        $"a.ipr_datfinimgprd", $"a.ipr_idtetaprd", $"a.ipr_idtcom", $"a.ipr_lbtypprd", $"a.ipr_voibpexta",
        $"a.ipr_comexta", $"a.ipr_codptlcomexta", $"a.ipr_lbpayexta", $"a.ipr_steutlexta", $"a.ipr_voibpextb",
        $"a.ipr_comextb", $"a.ipr_codptlcomextb", $"a.ipr_lbpayextb", $"a.ipr_steutlextb", $"b.tie_raiscltie",
        $"b.tie_sirtie", $"b.tie_codptlcomtie", $"a.ipr_datmescom", $"a.gar_lbcodgar", $"a.gar_lbplghor",
        $"b.tie_voibptie", $"b.tie_cmpvoitie", $"b.tie_comtie", $"b.tie_lbpaytie")

      logger.info("selectFieldsFromAllJoionsFinalResultset completed successfully")
      res
    }

  /**
   * Addind new column tie_addresstie.
   * @param join5out dataFrame
   * @return dataFrame
   */
  def addColumnToFinalResultset(join5out: DataFrame): DataFrame =
    {
      logger.debug("addColumnToFinalResultset started")
      /**
       * First adding a new column ('tie_addresstie') to the join utput dataframe.
       * Filtering out required fields from the newly created dataframe.
       */
      val res = join5out.withColumn("tie_addresstie", Utils.getConcatenated($"tie_voibptie",
        $"tie_cmpvoitie", $"tie_codptlcomtie", $"tie_comtie", $"tie_lbpaytie"))
        .select("eds_idteds", "eds_lbapteds", "eds_nomcrteds", "isr_idtsoures", "isr_datdebimgsoures",
          "isr_datfinimgsoures", "tie_idttie", "ipr_datdebimgprd", "ipr_datfinimgprd", "ipr_idtetaprd",
          "ipr_idtcom", "ipr_lbtypprd", "ipr_voibpexta", "ipr_comexta", "ipr_codptlcomexta",
          "ipr_lbpayexta", "ipr_steutlexta", "ipr_voibpextb", "ipr_comextb", "ipr_codptlcomextb",
          "ipr_lbpayextb", "ipr_steutlextb", "tie_raiscltie", "tie_sirtie", "tie_addresstie",
          "tie_codptlcomtie", "ipr_datmescom", "gar_lbcodgar", "gar_lbplghor")
      logger.info("addColumnToFinalResultset completed successfully")
      res
    }

  /**
   * UDF to generate support
   */
  val udfSupport = udf((router_role: String, support_nominal: String, support_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      support_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      support_secours
    else null)

  /**
   * UDF to generate service Support
   */
  val generateServiceSupport = udf((router_role: String, support_type_nominal: String, support_type_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      support_type_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      support_type_secours
    else null)

  /**
   * UDF to generate CollecteNominal
   */
  val generateCollecteNominal = udf((router_role: String, collecte_nominal: String, collecte_secours: String) =>
    if (router_role != null && "NOMINAL".equals(router_role))
      collecte_nominal
    else if (router_role != null && "SECOURS".equals(router_role))
      collecte_secours
    else null)

  /**
   * Joins gatherInput DataFrame and AllRapReference DataFrame and return output DataFrame.
   * @param gatherInput DataFrame
   * @param AllRapReference DataFrame
   * @return DataFrame
   */
  def joinGatherAndAllRapRef(gatherInput: DataFrame, AllRapReference: DataFrame): DataFrame =
    {
      /**
       * Joining gatherInput DataFrame and AllRapReference file.
       * Applied left outer join as all the records from gatherInput are required in output.
       *  Key from both the dataframes i 'rap'.
       */
      val res = gatherInput.join(AllRapReference,
        gatherInput("rap") === AllRapReference("rap"), "left_outer")
        .drop(AllRapReference("rap"))

      logger.info("joinGatherAndAllRapRef transformation completed successfully")
      res
    }

  /**
   * Add a new column 'ServiceSupport' in the given DataFrame.
   * @param joinGatherAllRapRef DataFrame
   * @return DataFrame
   */
  def addServiceSupport(joinGatherAllRapRef: DataFrame): DataFrame =
    {
      /**
       * Calling UDF 'generateServiceSupport'. This UDF will do calculations based on passed values and will
       *  find out value for 'service_support' field.
       */
      val res = joinGatherAllRapRef.withColumn("service_support",
        generateServiceSupport(joinGatherAllRapRef("router_role"),
          joinGatherAllRapRef("support_type_nominal"),
          joinGatherAllRapRef("support_type_secours")))

      logger.info("addServiceSupport transformation completed successfully")
      res
    }

  /**
   * Add a new column 'CollectNominal' in the given DataFrame.
   * @param transform1 DataFrame
   * @return DataFrame
   */
  def addCollectNominal(transform1: DataFrame): DataFrame =
    {
      /**
       * Calling UDF 'generateCollecteNominal'. This UDF will do calculations based on passed values and will
       *  find out value for 'collect' field.
       */
      val res = transform1.withColumn("collect",
        generateCollecteNominal(transform1("router_role"),
          transform1("collecte_nominal"),
          transform1("collecte_secours")))

      logger.info("addCollectNominal transformation completed successfully")
      res
    }

  /**
   * Add a new column 'Support' in the given DataFrame.
   * @param transform2 DataFrame
   * @return DataFrame
   */
  def addSupport(transform2: DataFrame): DataFrame =
    {
      /**
       * Calling UDF 'udfSupport'. This UDF will do calculations based on passed values and will
       *  find out value for support field.
       */
      val res = transform2.withColumn("support",
        udfSupport(transform2("router_role"),
          transform2("support_nominal"),
          transform2("support_secours")))
      logger.info("addSupport transformation completed successfully")
      res
    }
  /**
   * Rename column ville_extremite_A_new to ville_extremite_A.
   * @param transformReplace
   * @return
   */
  def renameColumnVilleExtremiteA(transformReplace: DataFrame): DataFrame =
    {
      /**
       * Field name required in the output file is different from the input file.
       * Replacing field name ville_extremite_A_new to ville_extremite_A.
       */
      val res = transformReplace.withColumnRenamed("ville_extremite_A_new",
        "ville_extremite_A")
      logger.info("renameColumnVilleExtremiteA transformation completed successfully")
      res

    }

  /**
   * Drop the 'LibelleRap' column from the given DataFrame.
   * @param deselect DataFrame
   * @return DataFrame
   */
  def dropColumnLibelleRap(deselect: DataFrame): DataFrame =
    {
      try {
        val res = deselect.drop("libelle_rap")
        logger.info("dropColumnLibelleRap transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Replacing 'new line' character value of IOS field to blank string from the given DataFrame.
   * @param wasac DataFrame
   * @return DataFrame
   */
  def changeIOSField(wasac: DataFrame): DataFrame =
    {
      try {
        /**
         * Search 'new line' character for 'ios' column and replace the character
         *  with blank string.
         */
        val res = wasac.na.replace("ios", Map("\n" -> ""))
        logger.info("changeIOSField transformation completed successfully")
        res
      } catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          throw e
        }
      }
    }

  /**
   * Join Wasac And Acort files
   * @param sortedWasacSkippedNewline
   * @param dfAcort DataFrame
   * @return DataFrame
   */
  def joinWasacAndAcort(sortedWasacSkippedNewline: DataFrame, dfAcort: DataFrame): DataFrame =
    {
      /**
       * Joining Wasac file and Acort file.
       * Applied left outer join as all the records from Wasac are required in output.
       *  Key from Wasac file is feuillet. Key from Acort file is element_reseau.
       */
      val res = sortedWasacSkippedNewline.join(dfAcort,
        sortedWasacSkippedNewline("feuillet") === dfAcort("element_reseau"),
        "left_outer")
      logger.info("changeIOSField transformation completed successfully")
      res
    }

  /**
   * Add new columns with blanks values into the dataframe.
   * @param deselectJoin DataFrame
   * @return DataFrame
   */
  def addBlankCols1(deselectJoin: DataFrame): DataFrame =
    {
      /**
       * Creating new columns in the dataframe.
       * New columns added - gtr, plage_horaire_gtr, raison_sociale, raison_sociale_ticket, addresse_complete_client,
       * code_postal_client, siren, societe_extremite_A, ville_extremite_A, cp_extremite_A, societe_extremite_B,
       * ville_extremite_B, voie_extremite_B, cp_extremite_B, population, population_cat, ios_version_from_iai
       * Default value for each column is set to empty string.
       */
      val res = deselectJoin.withColumn("gtr", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("plage_horaire_gtr", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("raison_sociale", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("raison_sociale_ticket", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("addresse_complete_client", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("code_postal_client", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("siren", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("societe_extremite_A", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("ville_extremite_A", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("voie_extremite_A", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("cp_extremite_A", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("societe_extremite_B", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("ville_extremite_B", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("voie_extremite_B", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("cp_extremite_B", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("population", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("population_cat", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("ios_version_from_iai", Utils.createNewBlankColumn(lit("": String)))

      logger.info("addBlankCols1 transformation completed successfully")
      res
    }

  /**
   * Add new columns into the dataframe with blanks values.
   * @param deselectJoin DataFrame
   * @return DataFrame
   */
  def addBlankCols2(deselectJoin: DataFrame): DataFrame =
    {
      /**
       * Creating new columns in the dataframe..
       * New columns - identifiant_eds_pilote, etat_produit, libelle_rap, type_produit, datemescom, service_support,
       * collect, support, nb_paires, collect_role.
       * Default value for each column is set to empty string.
       */
      val res = deselectJoin.withColumn("identifiant_eds_pilote", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("etat_produit", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("libelle_rap", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("type_produit", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("datemescom", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("service_support", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("collect", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("support", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("nb_paires", Utils.createNewBlankColumn(lit("": String)))
        .withColumn("collect_role", Utils.createNewBlankColumn(lit("": String)))

      logger.info("addBlankCols2 transformation completed successfully")
      res
    }

}