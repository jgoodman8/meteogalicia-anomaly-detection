package meteogalicia.services

import meteogalicia.model.CustomRow
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object MLService extends Logging {

  def trainKMeansModel(data: DataFrame): KMeansModel = {

    val models = 2 to 20 map { k =>
      new KMeans()
        .setK(k)
        .fit(data)
    }

    val costs = models.map(model => model.computeCost(data))
    val selected = elbowSelection(costs, 0.7)

    logInfo("Selecting k-means model: " + models(selected).k)

    models(selected)
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

  def getDistancesToCentroids(data: DataFrame, model: KMeansModel): RDD[Double] = {
    model
      .transform(data)
      .rdd
      .map(row => CustomRow(row))
      .map(customRow => {
        val centroid = model.clusterCenters(customRow.prediction)
        Vectors.sqdist(centroid, customRow.features)
      })
  }

}
