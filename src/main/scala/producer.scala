
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}

import org.apache.spark.sql.types._


object ProducerObj {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Producer").setMaster("local[2]")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val kafkaBrokers = "localhost:9092"

    import spark.implicits._

    // Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "spark-test")
      .option("startingOffsets", "earliest")
      .load()

    val kafkaData = kafka
      .withColumn("Key", $"key".cast(StringType))
      .withColumn("Topic", $"topic".cast(StringType))
      .withColumn("Offset", $"offset".cast(LongType))
      .withColumn("Partition", $"partition".cast(IntegerType))
      .withColumn("Timestamp", $"timestamp".cast(TimestampType))
      .withColumn("Value", $"value".cast(StringType))
      .select("Value")

    kafkaData.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
  }
}