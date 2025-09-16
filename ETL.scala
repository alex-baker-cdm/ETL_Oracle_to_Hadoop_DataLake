import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import collection.JavaConverters._
import org.apache.kudu.spark.kudu._
import sys.process._
import org.apache.spark.sql.functions.current_timestamp
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

val source_system = "_____"
val source_ip = "_______"
val source_user = "________"
val source_pass = "________"
val source_port = "_______"

val crm_jdbc_url = "jdbc:oracle:thin:" + source_user + "/" + source_pass + "//@" + source_ip + ":" + source_port + "/" + source_system
val kudu_master_nodes = "hddlm4.stc.com.bh:7051,hddlm5.stc.com.bh:7051,hddlm6.stc.com.bh:7051"

val import_query = """(Select
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
--'Spark Data Loading' as "bdl_source_pipelineid",
--cast(round((cast (systimestamp at time zone 'UTC' as date) - date '1970-01-01') * 86400)as VARCHAR(40)) as "bdl_source_pipeline_executionid",
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
    --and ast.last_upd between to_date('26-Feb-2023','DD-Mon-YYYY') and  to_date('02-Mar-2023','DD-Mon-YYYY')
    and ast.last_upd >= sysdate - 4/24
)"""

val df = spark.read.format("jdbc")
  .option("url", crm_jdbc_url)
  .option("dbtable", import_query)
  .option("user", source_user)
  .option("password", source_pass)
  .option("driver", "oracle.jdbc.driver.OracleDriver")
  .load()

def loadToKudu(dataFrame: DataFrame, tableName: String, kuduMasterNodes: String): Unit = {
  val kuduContext = new KuduContext(kuduMasterNodes, spark.sparkContext)
  kuduContext.upsertRows(dataFrame, s"impala::$tableName")
}

val tableName = "bdl_raw_qa.serviceaccountpackageplanlookup"
loadToKudu(df, tableName, kudu_master_nodes)
