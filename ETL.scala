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

val oracleConfig = OracleConfig(
  system = "_____",
  ip = "_______", 
  user = "________",
  pass = "________",
  port = "_______"
)

val kudu_master_nodes = "hddlm4.stc.com.bh:7051,hddlm5.stc.com.bh:7051,hddlm6.stc.com.bh:7051"

val extractLayer = new ExtractLayer(oracleConfig)
val rawDf = extractLayer.extractData(spark)

val df = rawDf.select(
  col("row_id").alias("assetid"),
  col("serv_acct_id").alias("subscriberid"),
  coalesce(col("duns_num"), col("serial_num")).alias("msisdn"),
  (col("created") + expr("3/24")).alias("activationdate"),
  when(col("sp_num").isNotNull, col("sp_num"))
    .otherwise(col("part_num")).alias("packageplanid"),
  when(col("status_cd") === "Inactive",
    coalesce(col("status_chg_dt"), col("last_upd")) + expr("3/24"))
    .otherwise(to_date(lit("2099-12-31"), "yyyy-MM-dd")).alias("deactivationdate"),
  col("status_cd").alias("statuscd"),
  col("body_style_cd").alias("prodtype"),
  col("last_upd"),
  current_timestamp().alias("bdl_created_date"),
  current_timestamp().alias("bdl_db_created_date"),
  lit("Spark Data Loading").alias("bdl_pipelineid"),
  lit("CRM|S_ASSET").alias("bdl_source")
)

val kuduContext = new KuduContext(kudu_master_nodes, spark.sparkContext)
val tableName = "bdl_raw_qa.serviceaccountpackageplanlookup"


kuduContext.upsertRows(df, s"impala::$tableName")
