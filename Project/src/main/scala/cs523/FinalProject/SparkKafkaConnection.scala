package cs523.FinalProject

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka


object SparkKafkaConnection {
  
  def main(args: Array[String]) {
    val ssc = GetStreamingContext()
    val kafkaStream = kafka.KafkaUtils.createStream(ssc, "localhost:2189","spark-streaming-consumer-group", Map("LogAnalytics" -> 5) )
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  def GetSparkConf(): SparkConf ={
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("NetworkWordCount")
    conf
  }

  def GetStreamingContext(): StreamingContext ={
    new StreamingContext(GetSparkConf(), Seconds(10))
  }
  
  
}