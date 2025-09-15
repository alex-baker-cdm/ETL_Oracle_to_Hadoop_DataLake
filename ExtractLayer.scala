import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Connection
import java.sql.DriverManager

case class OracleConfig(system: String, ip: String, user: String, pass: String, port: String) {
  def getJdbcUrl(): String = {
    s"jdbc:oracle:thin:$user/$pass//@$ip:$port/$system"
  }
}

class ExtractLayer(config: OracleConfig) {
  
  def getConnection(): Connection = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    DriverManager.getConnection(config.getJdbcUrl(), config.user, config.pass)
  }
  
  def extractData(spark: SparkSession, incrementalHours: Double = 4.0/24): DataFrame = {
    val simplifiedQuery = s"""(SELECT
      ast.row_id,
      ast.serv_acct_id,
      orgext.duns_num,
      ast.serial_num,
      ast.created,
      ast.sp_num,
      pint.part_num,
      ast.status_cd,
      ast.status_chg_dt,
      ast.last_upd,
      pint.body_style_cd,
      ast.prod_id
    FROM
      siebel.S_Asset ast,
      siebel.S_PROD_INT pint,
      siebel.S_ORG_EXT orgext
    WHERE
      ast.prod_id = pint.row_id
      AND ast.serv_acct_id = orgext.row_id
      AND pint.BODY_STYLE_CD = 'Service Plan'
      AND ast.serv_acct_id IS NOT NULL
      AND ast.last_upd >= sysdate - $incrementalHours
    )"""
    
    spark.read.format("jdbc")
      .option("url", config.getJdbcUrl())
      .option("dbtable", simplifiedQuery)
      .option("user", config.user)
      .option("password", config.pass)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
  }
}
