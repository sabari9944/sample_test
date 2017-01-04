package com.obs.pn.acq

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.StructType
import com.obs.pn.commons.Utils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

object Acquisition {
  
  /** Logger */
  val logger = LoggerFactory.getLogger(Acquisition.getClass)
  
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
   * Delimiter value while loading file
   */
  val SEMICOLON = ";"
  
  /**
   * QuoteMode for Saving
   */
 val QUOTE="quote"
 
 /**
   * QuoteMode Value for Saving
   */
 val TILDE="~" 
 /**
   * Delimiter value for storing data in hive table
   */
  val CARETS = "^"  

  /**
   * True value
   */
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
  
  /**
   * get file name suffix by timestamp from property
   */
  def getTimestampFilename(property: String, timestamp: String): String = {
    
    logger.debug(" Property : " + property)        
    val filename: String = Utils.prop.getString(property + ".dir") + timestamp + Utils.prop.getString(property + ".file") + timestamp + Utils.prop.getString("dev.fileExtension")
    logger.debug(" Filename : " + filename)        
    filename
  }
  
  /**
   * get file name suffix from property
   */
  def getFilename(property: String): String = {
    
    logger.debug(" Property : " + property)        
    val filename: String = Utils.prop.getString(property + ".dir") + Utils.prop.getString(property + ".file") + Utils.prop.getString("dev.fileExtension")
    logger.debug(" Filename : " + filename)        
    filename
  }
  
  /**
   * load timestamp file content from property
   */
  def loadTimestampFile( property: String, timestamp: String, univocity: Boolean): DataFrame = {
    val res = loadDataFile(getTimestampFilename( property, timestamp), univocity)
    res
  }
  
  /**
   * load timestamp file content from property
   */
  def loadFile( property: String, univocity: Boolean): DataFrame = {
    val res = loadDataFile(getFilename( property), univocity)
    res
  }
  
  /**
   * load data file from filename (HDFS directory)
   */
  def loadDataFile(fileName: String, withoutEncoder: Boolean): DataFrame =
    {
      
      try {
        
        logger.info("### Loading file: " + fileName)
        
        val reader = Utils.sqlContext.read
            .format(Acquisition.FORMAT);
          
        if(withoutEncoder) {
          reader.option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
            .option(Acquisition.INFERSCHEMA, Acquisition.TRUE) // Automatically infer data types
            .option(Acquisition.HEADER, Acquisition.TRUE) // Use first line of all files as header
              
        } else {
          reader.option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
            .option(Acquisition.INFERSCHEMA, Acquisition.TRUE)
            .option(Acquisition.HEADER, Acquisition.TRUE)
            .option(Acquisition.CHARSET,Acquisition.CHARSETVALUE)
        }

        val res = reader.load(fileName)
        
        res
      } catch {
        case e: Exception => {
          logger.error("### Unable to load file=" + fileName)
          throw e
        }
      }
    }
  
   /**
     * Write files to given location.
     */
   def writeFile(dataframe: DataFrame, fileName: String, header:Boolean): Unit =
      {
        try {
          val writer = dataframe.write.format(Acquisition.FORMAT)
          if(header){
            writer.option(Acquisition.HEADER, Acquisition.TRUE)
            .option(Acquisition.DELIMITER, Acquisition.SEMICOLON)
          }
          else{
            writer.option(Acquisition.QUOTE,Acquisition.TILDE)
            .option(Acquisition.DELIMITER,Acquisition.CARETS)
          }
            writer.mode(SaveMode.Overwrite)
            .save(fileName)
  
          logger.debug("-" + fileName + " write completed successfully"  )
  
        } catch {
          case e: Exception => {
            logger.error("Failed to write file="+fileName)
            throw e
          }
        }
      }  

}
