package cs523.FinalProject

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.log4j._

object SparkKafkaConn {
  
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf()
      .setAppName("NetworkWordCount")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkaStream = kafka.KafkaUtils.createStream(ssc, "localhost:2189","spark-streaming-consumer-group", Map("LogAnalytics" -> 5) )
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  
  
}