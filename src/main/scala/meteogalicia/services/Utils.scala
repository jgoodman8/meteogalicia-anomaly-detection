package meteogalicia.services

import java.io.{BufferedWriter, File, FileWriter}

import meteogalicia.model.PollutionGases

object Utils {
  def saveThreshold(threshold: Double, fileName: String): Unit = {
    val file = new File(fileName)
    val bufferWriter = new BufferedWriter(new FileWriter(file))
    bufferWriter.write(threshold.toString)
    bufferWriter.close()
  }

  def parsePollutionInput(values: String): (String, PollutionGases) = {
    val parsedPurchase = values.split(',')

    val pollutionGases = PollutionGases(
      parsedPurchase(0).toDouble,
      parsedPurchase(1).toDouble,
      parsedPurchase(2).toDouble,
      parsedPurchase(3).toDouble,
      parsedPurchase(4).toDouble,
      parsedPurchase(5).toDouble
    )

    (parsedPurchase(6), pollutionGases)
  }

  def pollution_to_string(pollutionGases: PollutionGases, date: String): String = {
    pollutionGases.CO + "," +
      pollutionGases.NO + "," +
      pollutionGases.SO2 + "," +
      pollutionGases.NOX + "," +
      pollutionGases.O3 + "," +
      pollutionGases.NO2 + "," +
      date
  }
}
