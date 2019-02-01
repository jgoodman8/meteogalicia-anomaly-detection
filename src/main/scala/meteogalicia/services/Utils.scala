package meteogalicia.services

import java.io.{BufferedWriter, File, FileWriter}

object Utils {
  def saveThreshold(threshold: Double, fileName: String): Unit = {
    val file = new File(fileName)
    val bufferWriter = new BufferedWriter(new FileWriter(file))
    bufferWriter.write(threshold.toString)
    bufferWriter.close()
  }
}
