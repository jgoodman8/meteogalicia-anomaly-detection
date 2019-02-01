package meteogalicia.jobs

import meteogalicia.services.{MLService, Utils}
import meteogalicia.services
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClusteringPipeline extends App {

  def runPipeline(sparkSession: SparkSession): Unit = {
    val data = loadData(sparkSession)
    val transformedData = createFeaturesFromData(sparkSession, data)
    val (model, threshold) = trainAndGetThreshold(transformedData)

    model.write.save(services.ConfigurationService.getModelPath)

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

  def createFeaturesFromData(sparkSession: SparkSession, data: DataFrame): DataFrame = {
    val inputFeatures = Array("CO", "NO", "NO2", "NOX", "O3", "SO2")

    new VectorAssembler()
      .setInputCols(inputFeatures)
      .setOutputCol("features")
      .transform(data)
  }

  def trainAndGetThreshold(data: DataFrame): (MLWritable, Double) = {
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
