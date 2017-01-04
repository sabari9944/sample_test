package com.obs.pn.ticketenrichi.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.obs.pn.commons.Utils.sqlContext.implicits._
import scala.reflect.runtime.universe

object CorrespondanceCodeInseecodePostalUtile {

  /**
   * Creating the schema for INSEE output
   */
  case class CorrespondanceCodeInseecodePostalUtileSchema(nom_commune: String, poulation_tot: String, code_insee: String, nom_commune_caps: String, nom_commune_caps2: String, code_postal: String)
  
  /**
   * Method to check length of the string and if length is 4 then concat the string with 0
   */
  def correctCodePostal(s: String): String =
    {
      if (s != null && s.length() == 5) s
      else if (s != null && s.length() == 4) "0".concat(s)
      else s
    }
	
  /**
   * Perform the transformation calling the method "correctCodePostal"
   */

  def transformCorrespondence(insee: DataFrame): DataFrame = insee.map(x => { CorrespondanceCodeInseecodePostalUtileSchema(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, correctCodePostal(x(5).toString)) }).toDF

}