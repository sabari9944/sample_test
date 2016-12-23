package com.obs.pn.acq.communesinseeutile

import com.obs.pn.acq.Acquisition
import org.apache.spark.sql.DataFrame
import com.obs.pn.commons.Utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Utils.sqlContext.implicits._

object CommunesInseeUtile {
   /* Creating the schema for INSEE output */
  case class CommunesInseeUtileSchema(nom_commune: String, poulation_tot: String, code_insee: String, nom_commune_caps: String, nom_commune_caps2: String)
  
  /**
   *  need to .replace("û", "u")
   */
  def correctNomCommune(s: String): String =
    {
      val result = s.replace("-", " ").replace("à¸£à¸›", "u")
      result
    }
  /**
   * Call the methods for transformation
   */
  def transform(communes: DataFrame): DataFrame = {
    val addCol = addCommunesCol(communes)
    val replaceNomCommuneCaps2 = replaceCommunesCol(addCol)
    return replaceNomCommuneCaps2
  }
  /**
   * Add new Column
   */
  def addCommunesCol(communes: DataFrame): DataFrame = {
    val addColumnInCommunes = communes.withColumn("nom_commune_caps2", $"nom_commune_caps").select("*")
    return addColumnInCommunes
  }

  /**
   * Replace column value
   */
  def replaceCommunesCol(communes: DataFrame): DataFrame = {
    val correctNonCommunes = communes.map(x => { CommunesInseeUtileSchema(x(0).toString, x(1).toString, x(2).toString, x(3).toString, correctNomCommune(x(4).toString)) }).toDF
    return correctNonCommunes
  }

}