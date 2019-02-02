package meteogalicia.jobs

import meteogalicia.services
import meteogalicia.services.{MLService, Utils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object ClusteringPipeline extends App {

  def runPipeline(sparkSession: SparkSession): Unit = {
    val data = loadData(sparkSession)
    val transformedData = createFeaturesFromData(sparkSession, data)
    val (model, threshold) = trainAndGetThreshold(transformedData)

    model.save(sparkSession.sparkContext, services.ConfigurationService.getModelPath)

    Utils.saveThreshold(threshold, services.ConfigurationService.getThresholdPath)
  }

  def loadData(sparkSession: SparkSession): DataFrame = {
    val fileRoute = services.ConfigurationService.getPollutionCSVPath

    val data = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fileRoute)

    data
  }

  def createFeaturesFromData(sparkSession: SparkSession, data: DataFrame): RDD[Vector] = {
    data
      .select("CO", "NO", "NO2", "NOX", "O3", "SO2")
      .rdd
      .map(row => {
        val buffer = ArrayBuffer[Double]()

        buffer.append(row.getAs[Double]("CO"))
        buffer.append(row.getAs[Double]("NO"))
        buffer.append(row.getAs[Double]("NO2"))
        buffer.append(row.getAs[Double]("NOX"))
        buffer.append(row.getAs[Double]("O3"))
        buffer.append(row.getAs[Double]("SO2"))

        Vectors.dense(buffer.toArray)
      })
  }

  def trainAndGetThreshold(data: RDD[Vector]): (Saveable, Double) = {
    val model = MLService.trainKMeansModel(data)
    val distances: RDD[Double] = MLService.getDistancesToCentroids(data, model)

    val threshold = distances.top(2000).last

    (model, threshold)
  }

  val sparkSession = SparkSession.builder()
    .appName("Clustering Pipeline")
    .master("local[4]")
    .getOrCreate()

  runPipeline(sparkSession)

  sparkSession.stop()
}
