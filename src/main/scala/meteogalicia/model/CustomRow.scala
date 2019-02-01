package meteogalicia.model

import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector

object CustomRow {
  def apply(row: Row): CustomRow = {
    val features = row.getAs[Vector]("features")
    val prediction = row.getAs[Int]("prediction")

    new CustomRow(features, prediction)
  }
}

case class CustomRow(features: Vector, prediction: Int)
