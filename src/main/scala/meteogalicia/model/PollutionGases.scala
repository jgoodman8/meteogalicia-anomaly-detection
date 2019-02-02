package meteogalicia.model

import org.apache.spark.sql.types.{DoubleType, StructType}

object PollutionGases {
  val schema: StructType = (new StructType)
    .add("CO", DoubleType, nullable = true)
    .add("NO", DoubleType, nullable = true)
    .add("NO2", DoubleType, nullable = true)
    .add("NOX", DoubleType, nullable = true)
    .add("O3", DoubleType, nullable = true)
    .add("SO2", DoubleType, nullable = true)
}

case class PollutionGasesTrain(CO: Double, NO: Double, NO2: Double, NOX: Double, O3: Double, SO2: Double)
case class PollutionGases(CO: Double, NO: Double, SO2: Double, NOX: Double, O3: Double, NO2: Double)
