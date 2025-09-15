import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

object KuduLoader {
  def loadToKudu(dataFrame: DataFrame, kuduMasterNodes: String, tableName: String, sparkContext: SparkContext): Unit = {
    val kuduContext = new KuduContext(kuduMasterNodes, sparkContext)
    kuduContext.upsertRows(dataFrame, s"impala::$tableName")
  }
}
