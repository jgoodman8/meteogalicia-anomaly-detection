package meteogalicia.services

import java.io.{BufferedWriter, File, FileWriter}

import meteogalicia.model.PollutionGases
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

object MLService {

  def trainKMeansModel(data: RDD[Vector]): KMeansModel = {

    val models = 2 to 20 map { k =>
      new KMeans()
        .setK(k)
        .run(data)
    }

    val costs = models.map(model => model.computeCost(data))
    val selectedK = elbowSelection(costs, 0.7)

    val file = new File("resources/k")
    val bufferWriter = new BufferedWriter(new FileWriter(file))
    bufferWriter.write(selectedK.toString)
    bufferWriter.close()

    models(selectedK)
  }

  def elbowSelection(costs: Seq[Double], ratio: Double): Int = {
    var k = 0

    for (index <- 1 until costs.length) {
      val ratio = costs(index) / costs(index - 1)

      if (ratio > 0.7 && k == 0) {
        k = index + 1
      }
    }

    k
  }

  def getDistancesToCentroids(data: RDD[Vector], model: KMeansModel): RDD[Double] = {
    data.map(instance => distToCentroids(instance, model))
  }

  def isAnomaly(pollutionGases: PollutionGases, model: KMeansModel, threshold: Double): Boolean = {
    val featuresBuffer = ArrayBuffer[Double]()

    featuresBuffer.append(pollutionGases.CO)
    featuresBuffer.append(pollutionGases.NO)
    featuresBuffer.append(pollutionGases.NO2)
    featuresBuffer.append(pollutionGases.NOX)
    featuresBuffer.append(pollutionGases.O3)
    featuresBuffer.append(pollutionGases.SO2)

    val features = Vectors.dense(featuresBuffer.toArray)

    val distance = distToCentroids(features, model)

    distance.>(threshold)
  }

  def distToCentroids(data: Vector, model: KMeansModel): Double = {
    val centroid = model.clusterCenters(model.predict(data)) // if more than 1 center
    Vectors.sqdist(data, centroid)
  }

  def detectAnomalies(sparkSession: SparkSession,
                      stream: DStream[(String, PollutionGases)],
                      model: KMeansModel,
                      threshold: Double): DStream[(String, PollutionGases)] = {
    stream
      .filter(tuple => {
        val pollutionGases: PollutionGases = tuple._2
        isAnomaly(pollutionGases, model, threshold)
      })
  }

  def loadModel(sparkSession: SparkSession): KMeansModel = {
    KMeansModel.load(sparkSession.sparkContext, ConfigurationService.getModelPath)
  }

  def loadThreshold(sparkSession: SparkSession): Double = {
    val rawData = sparkSession.sparkContext.textFile(ConfigurationService.getThresholdPath, 20)
    val threshold = rawData.map { line => line.toDouble }.first()

    threshold
  }
}
