package etl

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETLJob {
  
  def createJdbcDataFrame(
    spark: SparkSession,
    jdbcUrl: String,
    query: String,
    user: String,
    password: String
  ): DataFrame = {
    spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", query)
      .option("user", user)
      .option("password", password)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
  }
  
  def writeToKudu(
    df: DataFrame,
    kuduContext: KuduContext,
    tableName: String
  ): Unit = {
    kuduContext.upsertRows(df, s"impala::$tableName")
  }
  
  def buildImportQuery(): String = {
    """(Select
ast.row_id as "assetid",
ast.serv_acct_id AS "subscriberid",
nvl(orgext.duns_num,ast.serial_num) as "msisdn",
(ast.created +3/24) AS "activationdate",
(case when (ast.sp_num is not null)
      then ast.sp_num
      else pint.part_num
  end) AS "packageplanid" ,
CASE WHEN ast.status_cd = 'Inactive'
    THEN (NVL(ast.status_chg_dt,ast.last_upd)  + 3/24)
    ELSE TO_DATE('2099-12-31','YYYY-MM-DD')
    END AS "deactivationdate",
ast.status_cd AS "statuscd",
pint.body_style_cd "prodtype",
ast.last_upd as "last_upd",
SYSDATE as "bdl_created_date",
SYSDATE as "bdl_db_created_date",
'Spark Data Loading' as "bdl_pipelineid",
'CRM|S_ASSET' as "bdl_source"
from
    siebel.S_Asset ast,
    siebel.S_PROD_INT pint,
    siebel.S_ORG_EXT orgext
Where
    ast.prod_id = pint.row_id
    and ast.serv_acct_id = orgext.row_id
    and pint.BODY_STYLE_CD = 'Service Plan'
    and ast.serv_acct_id  is not null
    and ast.last_upd >= sysdate - 4/24
)"""
  }
  
  def runETL(
    spark: SparkSession,
    jdbcUrl: String,
    user: String,
    password: String,
    kuduMasterNodes: String,
    tableName: String
  ): Unit = {
    val query = buildImportQuery()
    val df = createJdbcDataFrame(spark, jdbcUrl, query, user, password)
    val kuduContext = new KuduContext(kuduMasterNodes, spark.sparkContext)
    writeToKudu(df, kuduContext, tableName)
  }
}
