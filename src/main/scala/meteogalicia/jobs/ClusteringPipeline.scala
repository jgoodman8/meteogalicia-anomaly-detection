package meteogalicia.jobs

import org.apache.spark.sql.SparkSession

object ClusteringPipeline extends App {


  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Clustering Pipeline")
    .getOrCreate()
}
