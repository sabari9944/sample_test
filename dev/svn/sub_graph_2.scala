package com.obs.pn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.ConfigFactory

object sub_graph_2 {
  def main(args:Array[String]){
   val conf= new SparkConf().setAppName("sub_graph_2")
   val sc= new SparkContext(conf)
   val sqlContext = new HiveContext(sc)
   val prop = ConfigFactory.load()
   
      //Loading ACORT file
   val dfAcort = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", prop.getString("dev.separator"))
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
     .load(prop.getString("dev.filter_acort"))
   
      //Loading incidentologie_wasac file
   val dfInc_wasac = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", prop.getString("dev.separator"))
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
     .load(prop.getString("dev.incidentologie_wasac"))
     
     //UDF for creating quoted field
      val createColumnWithQuote = udf((type_interne: String) => "\"" ) 
      
       //REMOVE newline characters
      val sortedWasacSkippedNewline = dfInc_wasac.na.replace("ios", Map("\n" -> ""))
  
  
      // WASAC record LEFT OUTER JOIN ACORT record
      val joinAcortWasac =  sortedWasacSkippedNewline.join(dfAcort, 
                            sortedWasacSkippedNewline("feuillet") === dfAcort("element_reseau"),
                           "left_outer")
                       
      //if type_interne is null replace it with null String ("null") 
      val replaced_joinAcortWasac_Df = joinAcortWasac.na.fill("null",Seq("type_interne"))
  
      //Filter (SELECT PART of filter) string_substring(type_interne,1,3)) == 'SU_'
      val select_Df = replaced_joinAcortWasac_Df.filter(
                      replaced_joinAcortWasac_Df("type_interne").startsWith("SU_"))  
      
      //Filter (DESELECT PART of filter)  type_interne(1,3) != "SU_"               
      val deselect_Df = replaced_joinAcortWasac_Df.filter(
                      !replaced_joinAcortWasac_Df("type_interne").startsWith("SU_")) 
                      
     // Columns num_pivot, element_reseau and type_interne need to be replaced; so dropping columns
     val  df_select_dropedCol =  select_Df.drop("num_pivot").drop("element_reseau").drop("type_interne")        
     
     //Recreating num_pivot, element_reseau and type_interne columns with quote values
     val df_select_quotedFields =  df_select_dropedCol
                                          .withColumn("num_pivot", createColumnWithQuote(lit("":String)))
                                          .withColumn("element_reseau", createColumnWithQuote(lit("":String)))
                                          .withColumn("type_interne", createColumnWithQuote(lit("":String))) 
     

          //  Lookup file TRONC     
          val TOPO_TRONC = (dfAcort.filter(dfAcort("type_interne").
                                              startsWith("TOPO_TRONC")))
          
         //  Lookup file si_SU 
          val si_SU = (dfAcort.filter(dfAcort("type_si") === "S-SU"))
         
          //  Lookup file TYPE_SI 
          val TYPE_SI = (dfAcort.filter(dfAcort("type_si").startsWith("A-Z")))         
         
          //Selecting (filtering) only required columns from lookup file for join
          val lk_TRONC = TOPO_TRONC.select(
                                       TOPO_TRONC("num_pivot") as("TRONC_num_pivot"),
                                       TOPO_TRONC("element_reseau") as("TRONC_element_reseau"),
                                       TOPO_TRONC("type_interne") as("TRONC_type_interne"))
                                       
          val lk_si_SU = si_SU.select(
                                       si_SU("num_pivot") as("SISU_num_pivot"),
                                       si_SU("element_reseau") as("SISU_element_reseau"),
                                       si_SU("type_interne") as("SISU_type_interne"))
                                       
          val lk_TYPE_SI = TYPE_SI.select(
                                       TYPE_SI("num_pivot") as("TYPE_SI_num_pivot"),
                                       TYPE_SI("element_reseau") as("TYPE_SI_element_reseau"),
                                       TYPE_SI("type_interne") as("TYPE_SI_type_interne")) 
                                       
              
              // De-selected Input file join with lk_TRONC
              val join2 =  deselect_Df.join(lk_TRONC, deselect_Df("num_pivot") === lk_TRONC("TRONC_num_pivot"), "left_outer")  
              
              // De-selected Input file join with lk_lk_si_SU
              val join3 =  join2.join(lk_si_SU, join2("num_pivot") === lk_si_SU("SISU_num_pivot"), "left_outer") 
              
              // De-selected Input file join with lk_TYPE_S
              val join4 = join3.join(lk_TYPE_SI, join3("num_pivot") === lk_TYPE_SI("TYPE_SI_num_pivot"), "left_outer")

              val acortTypInternArr = dfAcort.groupBy(dfAcort("num_pivot") as "accort_num_pivot")
                      .agg(expr("collect_list(type_interne) AS type_interne_array"), 
                           expr("collect_list(element_reseau) AS element_reseau_array")) 
                      
              val join5 = join4.join(acortTypInternArr, join4("num_pivot") === acortTypInternArr("accort_num_pivot"), 
                          "left_outer")        

            //This udf returns blank value. Used to create column with blank values.  
            val createNewBlankColumn = udf((type_interne: String) => None: Option[String] ) 
            
            //UDF to generate "rap" column value
            val calculate_rap = udf((type_interne_siSU: String, type_interne_SI: String) =>
              if (type_interne_siSU != null && type_interne_siSU.length() >= 10) type_interne_siSU.substring(3, 10)
              else if (type_interne_SI != null) type_interne_SI
              else null)
                            
              //UDF to generate ce_id 
            val calculate_CE_ID = udf((type_interne: Seq[String] , element_reseau: Seq[String]) =>
              if (type_interne != null && element_reseau != null && type_interne.indexOf("RSS_TYP160") != -1) 
                  element_reseau(type_interne.indexOf("RSS_TYP160"))
              else if (type_interne != null && type_interne.indexOf("R_TRL") != -1 ) 
                  element_reseau(type_interne.indexOf("R_TRL"))
              else null)
            
            //UDF to generate connexion_id  
            val calculate_connexion_id = udf((type_interne: String, element_reseau: String) => 
            if (type_interne != null && 
              "RSS_TYP164".equals(type_interne) || 
              "RSS_TYP180".equals(type_interne) || 
              "RSS_TYP220".equals(type_interne) ||
              "RSS_TYP210".equals(type_interne) ||
              "RSS_TYP165".equals(type_interne) ||
              "A_AFR".equals(type_interne) ||
              "A_IAP".equals(type_interne) ||
              "A_PHD".equals(type_interne)
              ) element_reseau
            else null)
      
      //UDF to generate support_id      
      val  calculate_support_id = udf((type_interne: String, element_reseau: String) => 
        if (type_interne != null && 
            "RSS_TYP163".equals(type_interne) || 
            "RSS_TYP178".equals(type_interne) || 
            "RSS_TYP209".equals(type_interne) ||
            "RSS_TYP180".equals(type_interne) 
            ) element_reseau
        else null) 
     
     //UDF to generate router_role 
     val calculate_router_role = udf((type_interne: String,role: String) => 
      if (type_interne != null && "RSS_TYP160".equals(type_interne) ) role 
      else null)  
      
      //UDF to generate dslam_id
     val calculate_dslam_id = udf((type_interne: Seq[String] , element_reseau: Seq[String]) => 
      if (type_interne != null && type_interne.indexOf("TOPO_DSLAM") != -1) 
        element_reseau(type_interne.indexOf("TOPO_DSLAM"))    //"TOPO_DSLAM"
      else null) 
      
      //UDF to generate nortel_id
     val calculate_nortel_id = udf((type_interne: Seq[String] , element_reseau: Seq[String]) => 
      if (type_interne != null && type_interne.indexOf("TOPO_NORTEL") != -1) 
        element_reseau(type_interne.indexOf("TOPO_NORTEL"))    //"TOPO_NORTEL"
      else null)

      //Generate pe_id
    val calculate_pe_id = udf((type_interne: Seq[String] , element_reseau: Seq[String]) =>
      if (type_interne != null && type_interne.indexOf("TOPO_PE") != -1) 
          element_reseau(type_interne.indexOf("TOPO_PE"))
      else null) 
      
       //UDF to generate fav_id
     val calculate_fav_id = udf((type_interne: Seq[String], element_reseau: Seq[String]) => 
      if (type_interne != null &&  type_interne.indexOf("TOPO_FAV") != -1  ) 
         element_reseau(type_interne.indexOf("TOPO_FAV"))   //"TOPO_FAV"
      else null) 
      
       //UDF to generate ntu_id
     val calculate_ntu_id = udf((type_interne: Seq[String], element_reseau: Seq[String]) => 
      if (type_interne != null &&  type_interne.indexOf("TOPO_NTU") != -1 )   //"TOPO_NTU"
        element_reseau(type_interne.indexOf("TOPO_NTU"))
      else null)
      
      //UDF to generate tronc_type
     val calculate_tronc_type = udf((type_interne: Seq[String]) => 
        if (type_interne != null &&  type_interne.indexOf("TOPO_TRONC") != -1 ) "TOPO_TRONC"  //"TOPO_TRONC"
        else null) 
      
      
      //Deselect Transformations. Creating new columns by calling related UDFs
      val newdf1 = join5.withColumn("rap", calculate_rap(join5("SISU_type_interne"), join5("TYPE_SI_type_interne")))
      
      val newdf2 = newdf1.withColumn("ce_id", 
                                     calculate_CE_ID(join5("type_interne_array"), 
                                     join5("element_reseau_array") ))

      val newdf3 = newdf2.withColumn("connexion_id",
          calculate_connexion_id(join5("type_interne"), join5("element_reseau")))
    
      val newdf4 = newdf3.withColumn("support_id",
          calculate_support_id(join5("type_interne"), join5("element_reseau")))
    
      val newdf5 = newdf4.withColumn("router_role",
          calculate_router_role(join5("type_interne"), join5("role"))) 
      
      //Adding columns with blank values.    
      val newdf5_1 =   newdf5.withColumn("gtr", createNewBlankColumn(lit("":String)))
                           .withColumn("plage_horaire_gtr", createNewBlankColumn(lit("":String)))
                           .withColumn("raison_sociale", createNewBlankColumn(lit("":String)))
                           .withColumn("raison_sociale_ticket", createNewBlankColumn(lit("":String)))
                           .withColumn("addresse_complete_client", createNewBlankColumn(lit("":String)))
                           .withColumn("code_postal_client", createNewBlankColumn(lit("":String)))
                           .withColumn("siren", createNewBlankColumn(lit("":String)))
                           .withColumn("societe_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("ville_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("voie_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("cp_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("societe_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("ville_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("voie_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("cp_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("population", createNewBlankColumn(lit("":String)))
                           .withColumn("population_cat", createNewBlankColumn(lit("":String)))
                           .withColumn("ios_version_from_iai", createNewBlankColumn(lit("":String)))
                        
      val newdf6 = newdf5_1.withColumn("dslam_id", 
                         calculate_dslam_id(join5("type_interne_array"), join5("element_reseau_array")))
                         
      val newdf6_1 =   newdf6.withColumn("master_dslam_id", createNewBlankColumn(join5("type_interne")))
        
      val newdf7 = newdf6_1.withColumn("nortel_id", 
                         calculate_nortel_id(join5("type_interne_array"), join5("element_reseau_array")))

    val newdf8 = newdf7.withColumn("pe_id",
      calculate_pe_id(join5("type_interne_array"), join5("element_reseau_array")))

    val newdf9 = newdf8.withColumn("fav_id",
      calculate_fav_id(join5("type_interne_array"), join5("element_reseau_array")))

    val newdf10 = newdf9.withColumn("ntu_id",
      calculate_ntu_id(join5("type_interne_array"), join5("element_reseau_array"))) 
                         
            val newdf11 = newdf10.withColumn("tronc_type", 
                         calculate_tronc_type(join5("type_interne_array"))) 
      
       val newdf12 = newdf11;                   
                         
      //Adding columns with blank values.         
      val newdf13 = newdf12.withColumn("identifiant_eds_pilote", createNewBlankColumn(lit("":String)))
                        .withColumn("etat_produit", createNewBlankColumn(lit("":String)))
                        .withColumn("libelle_rap", createNewBlankColumn(lit("":String)))
                        .withColumn("type_produit", createNewBlankColumn(lit("":String)))
                        .withColumn("datemescom", createNewBlankColumn(lit("":String)))
                        .withColumn("service_support", createNewBlankColumn(lit("":String)))
                        .withColumn("collect", createNewBlankColumn(lit("":String)))
                        .withColumn("support", createNewBlankColumn(lit("":String)))
                        .withColumn("nb_paires", createNewBlankColumn(lit("":String)))
                        .withColumn("collect_role", createNewBlankColumn(lit("":String)))
      
      //Renaming columns                  
      val newdf14 = newdf13.withColumnRenamed("feuillet", "identifiant_1_produit") 
                         .withColumnRenamed("ios", "ios_version")
                         
       val select1 = df_select_quotedFields.withColumnRenamed("feuillet", "identifiant_1_produit") 
                         .withColumnRenamed("ios", "ios_version")
        
       //Adding columns with blank values.  
       val select2 = select1.withColumn("gtr", createNewBlankColumn(lit("":String)))
                           .withColumn("plage_horaire_gtr", createNewBlankColumn(lit("":String)))
                           .withColumn("raison_sociale", createNewBlankColumn(lit("":String)))
                           .withColumn("raison_sociale_ticket", createNewBlankColumn(lit("":String)))
                           .withColumn("addresse_complete_client", createNewBlankColumn(lit("":String)))
                           .withColumn("code_postal_client", createNewBlankColumn(lit("":String)))
                           .withColumn("siren", createNewBlankColumn(lit("":String)))
                           .withColumn("societe_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("ville_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("voie_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("cp_extremite_A", createNewBlankColumn(lit("":String)))
                           .withColumn("societe_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("ville_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("voie_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("cp_extremite_B", createNewBlankColumn(lit("":String)))
                           .withColumn("population", createNewBlankColumn(lit("":String)))
                           .withColumn("population_cat", createNewBlankColumn(lit("":String)))
                           .withColumn("ios_version_from_iai", createNewBlankColumn(lit("":String)))
                           .withColumn("identifiant_eds_pilote", createNewBlankColumn(lit("":String)))
                            .withColumn("etat_produit", createNewBlankColumn(lit("":String)))
                            .withColumn("libelle_rap", createNewBlankColumn(lit("":String)))
                            .withColumn("type_produit", createNewBlankColumn(lit("":String)))
                            .withColumn("datemescom", createNewBlankColumn(lit("":String)))
                            .withColumn("service_support", createNewBlankColumn(lit("":String)))
                            .withColumn("collect", createNewBlankColumn(lit("":String)))
                            .withColumn("support", createNewBlankColumn(lit("":String)))
                            .withColumn("nb_paires", createNewBlankColumn(lit("":String)))
                            .withColumn("collect_role", createNewBlankColumn(lit("":String)))
                            .withColumn("master_dslam_id", createNewBlankColumn(lit("":String)))
                            .withColumn("rap", createNewBlankColumn(lit("":String)))
                            .withColumn("ce_id", createNewBlankColumn(lit("":String)))
                            .withColumn("connexion_id", createNewBlankColumn(lit("":String)))
                            .withColumn("support_id", createNewBlankColumn(lit("":String)))
                            .withColumn("router_role", createNewBlankColumn(lit("":String)))
                            .withColumn("dslam_id", createNewBlankColumn(lit("":String)))
                            .withColumn("nortel_id", createNewBlankColumn(lit("":String)))
                            .withColumn("pe_id", createNewBlankColumn(lit("":String)))
                            .withColumn("fav_id", createNewBlankColumn(lit("":String)))
                            .withColumn("ntu_id", createNewBlankColumn(lit("":String)))
                            .withColumn("tronc_type", createNewBlankColumn(lit("":String)))
                            
                              
                         
      //Selecting only required columns
      val deSELECT = newdf14.select("identifiant_1_produit","ip_admin","ios_version","constructeur", "chassis",
                 "rsc","version_boot","num_pivot","element_reseau","type_interne",
                 "identifiant_eds_pilote","etat_produit","rap","libelle_rap","type_produit",
                 "datemescom","ce_id","connexion_id","support_id","service_support",
                 "collect","support","nb_paires","collect_role","router_role",
                 "gtr","plage_horaire_gtr","raison_sociale","raison_sociale_ticket","addresse_complete_client",
                 "code_postal_client","siren","societe_extremite_A","ville_extremite_A","voie_extremite_A",
                 "cp_extremite_A","societe_extremite_B","ville_extremite_B","voie_extremite_B","cp_extremite_B",
                 "population","population_cat","ios_version_from_iai","dslam_id","master_dslam_id",
                 "nortel_id","pe_id","fav_id","ntu_id","tronc_type")
                 
     
      //Gather_Target_MARINE2 Part        
                 
      //Gather UDFs
      val udf_replaceST = udf((input: String) => 
                          if (input != null &&  input.startsWith("ST ") ) 
                              input.replace("ST ",  "SAINT ") 
                          else input)
   
     val udf_service_support = udf((router_role: String, support_type_nominal: String, support_type_secours: String) => 
                            if (router_role != null &&  "NOMINAL".equals(router_role) ) 
                                support_type_nominal 
                            else if (router_role != null &&  "SECOURS".equals(router_role) ) 
                                support_type_secours 
                            else null)
                

     val  udf_collecte_nominal = udf((router_role: String, collecte_nominal: String, collecte_secours: String) => 
                              if (router_role != null &&  "NOMINAL".equals(router_role) ) 
                                  collecte_nominal
                              else if (router_role != null &&  "SECOURS".equals(router_role) ) 
                                  collecte_secours 
                              else null)
   
                
     val udf_support =  udf((router_role: String, collecte_nominal: String, collecte_secours: String) => 
                        if (router_role != null &&  "NOMINAL".equals(router_role) ) 
                            collecte_nominal
                        else if (router_role != null &&  "SECOURS".equals(router_role) ) 
                            collecte_secours 
                        else null)
      
          //Loading AllRapReference file
          val AllRapReference = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .option("delimiter", prop.getString("dev.separator"))
            .option("parserLib", "univocity")
            .load(prop.getString("dev.rapReference"))
            
                   val gatherInput = deSELECT.drop("libelle_rap")   
            
            //Calling gather UDFs to add new columns
            val joinGatherAllRapRef = gatherInput.join(AllRapReference, 
                                    gatherInput("rap") === AllRapReference("rap"), "left_outer") 
                                    .drop(AllRapReference("rap")) // Join DF will contains duplicate column name 'rap'
              
            val transform1 = joinGatherAllRapRef.withColumn("service_support", 
                                udf_service_support(joinGatherAllRapRef("router_role"),
                                  joinGatherAllRapRef("support_type_nominal"),
                                  joinGatherAllRapRef("support_type_secours"))) 
                                    
            val transform2 = transform1.withColumn("collect", 
                                udf_collecte_nominal(transform1("router_role"),
                                    transform1("collecte_nominal"),
                                    transform1("collecte_secours")))  
                                    
            val transform3 = transform2.withColumn("support", 
                                udf_support(transform2("router_role"),
                                    transform2("collecte_nominal"),
                                    transform2("collecte_secours"))) 
                                    
            val transformReplace = transform3.withColumn("ville_extremite_A_new", 
                                          udf_replaceST(transform3("ville_extremite_A")))
                                          .drop(transform3("ville_extremite_A"))
            
            val replace =   transformReplace.withColumnRenamed("ville_extremite_A_new", 
                                      "ville_extremite_A")  
            
            //Save the sub_graph_2_output
            replace.write.format("com.databricks.spark.csv")
                      .option("header","true")
                      .option("delimiter", prop.getString("dev.separator")).mode(SaveMode.Overwrite)
                      .save(prop.getString("dev.sub_graph_2_output"))
            
          
      sc.stop()      
    }                                      
}