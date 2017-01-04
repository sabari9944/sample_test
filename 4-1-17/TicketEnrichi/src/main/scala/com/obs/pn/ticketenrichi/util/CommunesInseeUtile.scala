package com.obs.pn.ticketenrichi.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.obs.pn.commons.Utils.sqlContext.implicits._
import scala.reflect.runtime.universe

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
    replaceCommunesCol(addCol)
  }
  
  /**
   * Add new Column
   */
  def addCommunesCol(communes: DataFrame): DataFrame = communes.withColumn("nom_commune_caps2", $"nom_commune_caps").select("*")

  /**
   * Replace column value
   */
  def replaceCommunesCol(communes: DataFrame): DataFrame = communes.map(x => { CommunesInseeUtileSchema(x(0).toString, x(1).toString, x(2).toString, x(3).toString, correctNomCommune(x(4).toString)) }).toDF
    
}