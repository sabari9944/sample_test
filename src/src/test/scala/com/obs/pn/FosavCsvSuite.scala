import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import com.databricks.spark.csv._
import com.holdenkarau.spark.testing.SharedSparkContext
import scala.util.Try

class FosavCsvSuite extends FunSuite with SharedSparkContext   {
  

  val fosavfile = """src/test/ressources/incidentologie_FOSAV.csv"""
  val wasacfile ="""src/test/ressources/incidentologie_wasac.csv"""

  val fosav_count = 6464
  
  val wasac_count= 522128
  
  val schema = {
    
  }
  
  private var sqlContext: SQLContext = _
  
  
  test("Check the dataframe created from fosav matches the count"){
  val sqlContext = new SQLContext(sc)
    val results_fosav=sqlContext.csvFile(fosavfile, true, ';').select("detail_pbm").collect()
    assert(results_fosav.size === fosav_count)
    
    }
  
  
  
    test("Check the dataframe created from wasac matches the count"){
      val sqlContext = new SQLContext(sc)
    val results_wasac = sqlContext.csvFile(wasacfile, true, ';').select("ios").collect()
    
    
    assert(results_wasac.size === wasac_count)
    }
  
    
    
    test("Check the Column is renamed in Fosav"){
      val sqlContext = new SQLContext(sc)
      val results_col=sqlContext.csvFile(fosavfile, true, ';').select("pht_idtcom").withColumnRenamed("pht_idtcom", "feuillet")
     
    
      assert(Try(results_col("feuillet")).isSuccess)
      assert(Try(results_col("pht_idtcom")).isFailure)
      
      
   }
  
  
  
  
}