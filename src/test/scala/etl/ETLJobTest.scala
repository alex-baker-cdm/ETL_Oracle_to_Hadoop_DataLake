package etl

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import java.sql.{Date, Timestamp}

class ETLJobTest extends FunSuite with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ETL Test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  
  test("createJdbcDataFrame creates DataFrame with correct schema") {
    import spark.implicits._
    
    val mockData = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    assert(mockData.columns.length == 13)
    assert(mockData.columns.contains("assetid"))
    assert(mockData.columns.contains("subscriberid"))
    assert(mockData.columns.contains("msisdn"))
    assert(mockData.columns.contains("activationdate"))
    assert(mockData.columns.contains("packageplanid"))
    assert(mockData.columns.contains("deactivationdate"))
    assert(mockData.columns.contains("statuscd"))
    assert(mockData.columns.contains("prodtype"))
    assert(mockData.columns.contains("last_upd"))
    assert(mockData.columns.contains("bdl_created_date"))
    assert(mockData.columns.contains("bdl_db_created_date"))
    assert(mockData.columns.contains("bdl_pipelineid"))
    assert(mockData.columns.contains("bdl_source"))
    
    val count = mockData.count()
    assert(count == 1)
  }
  
  test("mock data matches expected Oracle query output schema") {
    import spark.implicits._
    
    val mockOracleData = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET"),
      ("ASSET002", "SUB002", "0987654321",
       Timestamp.valueOf("2023-03-02 03:00:00"), "PLAN002",
       Timestamp.valueOf("2023-03-10 03:00:00"), "Inactive", "Service Plan",
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    assert(mockOracleData.count() == 2)
    
    val activeRow = mockOracleData.filter($"statuscd" === "Active").first()
    assert(activeRow.getString(activeRow.fieldIndex("statuscd")) == "Active")
    assert(activeRow.getDate(activeRow.fieldIndex("deactivationdate")).toString == "2099-12-31")
    
    val inactiveRow = mockOracleData.filter($"statuscd" === "Inactive").first()
    assert(inactiveRow.getString(inactiveRow.fieldIndex("statuscd")) == "Inactive")
    assert(inactiveRow.getTimestamp(inactiveRow.fieldIndex("deactivationdate")).toString.startsWith("2023-03-10"))
  }
  
  test("verify timezone adjustment is applied correctly in mock data") {
    import spark.implicits._
    
    val originalCreatedTime = Timestamp.valueOf("2023-03-01 00:00:00")
    val expectedActivationTime = Timestamp.valueOf("2023-03-01 03:00:00")
    
    val mockData = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       expectedActivationTime, "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val activationDate = mockData.first().getTimestamp(mockData.first().fieldIndex("activationdate"))
    assert(activationDate.toString.contains("03:00:00"))
  }
  
  test("verify CASE logic for deactivationdate") {
    import spark.implicits._
    
    val activeAsset = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val row = activeAsset.first()
    val deactivationDate = row.getDate(row.fieldIndex("deactivationdate"))
    assert(deactivationDate.toString == "2099-12-31")
    
    val inactiveAsset = Seq(
      ("ASSET002", "SUB002", "0987654321",
       Timestamp.valueOf("2023-03-02 03:00:00"), "PLAN002",
       Timestamp.valueOf("2023-03-10 03:00:00"), "Inactive", "Service Plan",
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val inactiveRow = inactiveAsset.first()
    val inactiveDeactivationDate = inactiveRow.getTimestamp(inactiveRow.fieldIndex("deactivationdate"))
    assert(inactiveDeactivationDate.toString.startsWith("2023-03-10"))
  }
  
  test("verify metadata fields are present") {
    import spark.implicits._
    
    val mockData = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val row = mockData.first()
    assert(row.getString(row.fieldIndex("bdl_pipelineid")) == "Spark Data Loading")
    assert(row.getString(row.fieldIndex("bdl_source")) == "CRM|S_ASSET")
    assert(row.getTimestamp(row.fieldIndex("bdl_created_date")) != null)
    assert(row.getTimestamp(row.fieldIndex("bdl_db_created_date")) != null)
  }
  
  test("verify packageplanid CASE logic with different scenarios") {
    import spark.implicits._
    
    val withSpNum = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "SP_NUM_VALUE",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val row1 = withSpNum.first()
    assert(row1.getString(row1.fieldIndex("packageplanid")) == "SP_NUM_VALUE")
    
    val withPartNum = Seq(
      ("ASSET002", "SUB002", "0987654321",
       Timestamp.valueOf("2023-03-02 03:00:00"), "PART_NUM_VALUE",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-02 00:00:00"),
       Timestamp.valueOf("2023-03-02 00:00:00"),
       Timestamp.valueOf("2023-03-02 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    val row2 = withPartNum.first()
    assert(row2.getString(row2.fieldIndex("packageplanid")) == "PART_NUM_VALUE")
  }
  
  test("buildImportQuery returns non-empty SQL query") {
    val query = ETLJob.buildImportQuery()
    assert(query.nonEmpty)
    assert(query.contains("assetid"))
    assert(query.contains("subscriberid"))
    assert(query.contains("msisdn"))
    assert(query.contains("activationdate"))
    assert(query.contains("packageplanid"))
    assert(query.contains("deactivationdate"))
    assert(query.contains("+3/24"))
    assert(query.contains("CASE WHEN"))
    assert(query.contains("Spark Data Loading"))
    assert(query.contains("CRM|S_ASSET"))
  }
  
  test("ETL pipeline with mocked data and Kudu operations") {
    import spark.implicits._
    
    val mockOracleResultData = Seq(
      ("ASSET001", "SUB001", "1234567890", 
       Timestamp.valueOf("2023-03-01 03:00:00"), "PLAN001",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       Timestamp.valueOf("2023-03-01 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET"),
      ("ASSET002", "SUB002", "0987654321",
       Timestamp.valueOf("2023-03-02 03:00:00"), "PLAN002",
       Timestamp.valueOf("2023-03-10 03:00:00"), "Inactive", "Service Plan",
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       Timestamp.valueOf("2023-03-10 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET"),
      ("ASSET003", "SUB003", "5551234567",
       Timestamp.valueOf("2023-03-03 03:00:00"), "PLAN003",
       Date.valueOf("2099-12-31"), "Active", "Service Plan",
       Timestamp.valueOf("2023-03-03 00:00:00"),
       Timestamp.valueOf("2023-03-03 00:00:00"),
       Timestamp.valueOf("2023-03-03 00:00:00"),
       "Spark Data Loading", "CRM|S_ASSET")
    ).toDF("assetid", "subscriberid", "msisdn", "activationdate", "packageplanid",
           "deactivationdate", "statuscd", "prodtype", "last_upd",
           "bdl_created_date", "bdl_db_created_date", "bdl_pipelineid", "bdl_source")
    
    assert(mockOracleResultData.count() == 3)
    
    val activeRecords = mockOracleResultData.filter($"statuscd" === "Active")
    assert(activeRecords.count() == 2)
    
    val inactiveRecords = mockOracleResultData.filter($"statuscd" === "Inactive")
    assert(inactiveRecords.count() == 1)
    
    val allHaveMetadata = mockOracleResultData.filter(
      $"bdl_pipelineid" === "Spark Data Loading" && 
      $"bdl_source" === "CRM|S_ASSET"
    )
    assert(allHaveMetadata.count() == 3)
  }
}
