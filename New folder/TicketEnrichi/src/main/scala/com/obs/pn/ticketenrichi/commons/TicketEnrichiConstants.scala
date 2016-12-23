package com.obs.pn.ticketenrichi.commons

object TicketEnrichiConstants {
  /**
   * Constants for loading of files
   */
  val FORMAT = "com.databricks.spark.csv"
  /**
   * Option to specify the delimiter of csv file
   */
  val DELIMITER = "delimiter" 
  /**
   * Option in databricks to specify while reading or writing the file the header to be included or not
   */
  val HEADER = "header"
  /**
   * To infer column types
   */
  val INFERSCHEMA = "inferSchema"
  /**
   * delimiter value while loading file
   */
  val SEMICOLON = ";"
  val TRUE = "true"
  /**
   * Library for csv parsing
   * 
   */
  val PARSERLIB = "parserLib"
  /**
   * value to the library used for csv parsing.
  */
  val UNIVOCITY = "univocity"
  /**
   * Charset while reading the file.
   */
  val CHARSET = "charset"
  /**
   * Charset value. Default UTF-8
   */
  val CHARSETVALUE = "iso-8859-15"
}
