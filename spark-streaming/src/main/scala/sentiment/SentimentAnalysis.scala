package sentiment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SentimentAnalysis {

  // ─── Sentiment keyword lists ──────────────────────────────────────────────
  val positiveWords = Set("good", "happy", "love", "great", "awesome",
                          "excellent", "amazing", "fantastic", "wonderful", "best")
  val negativeWords = Set("bad", "sad", "hate", "worst", "terrible",
                          "horrible", "awful", "disgusting", "poor", "ugly")

  // ─── UDF: classify a tweet as Positive / Negative / Neutral ──────────────
  val classifyTweet = udf((text: String) => {
    if (text == null || text.trim.isEmpty) {
      "Neutral"
    } else {
      val words      = text.toLowerCase.replaceAll("[^a-z\\s]", "").split("\\s+").toSet
      val posMatches = words.intersect(positiveWords).size
      val negMatches = words.intersect(negativeWords).size

      if (posMatches > negMatches)      "Positive ✅"
      else if (negMatches > posMatches) "Negative ❌"
      else                              "Neutral  ➖"
    }
  })

  def main(args: Array[String]): Unit = {

    // ─── Build SparkSession ───────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Real-Time Twitter Sentiment Analysis")
      .master("local[*]")                          // run locally, use all cores
      .config("spark.sql.shuffle.partitions", "2") // keep it small for local mode
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")         // suppress INFO noise

    import spark.implicits._

    println(
      """
        |╔══════════════════════════════════════════════════════╗
        |║   Real-Time Twitter Sentiment Analysis               ║
        |║   Kafka → Spark Structured Streaming → Console       ║
        |╚══════════════════════════════════════════════════════╝
        |""".stripMargin)

    // ─── Read stream from Kafka ───────────────────────────────────────────
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter-topic")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // ─── Extract & clean tweet text ───────────────────────────────────────
    val tweets = kafkaStream
      .selectExpr("CAST(value AS STRING) AS raw_tweet")
      .withColumn("tweet",
        trim(regexp_replace(col("raw_tweet"), "[^a-zA-Z0-9 !?.,'\"@#]", "")))
      .filter(col("tweet") =!= "")

    // ─── Apply sentiment classification ───────────────────────────────────
    val analyzed = tweets
      .withColumn("sentiment", classifyTweet(col("tweet")))

    // ─── Format output line ───────────────────────────────────────────────
    val output = analyzed.select(
      concat(
        lit("Tweet : "), col("tweet"),
        lit("  →  "),    col("sentiment")
      ).alias("result")
    )

    // ─── Write to console ─────────────────────────────────────────────────
    val query = output.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("[INFO] Listening to Kafka topic 'twitter-topic' ... (Ctrl+C to stop)\n")
    query.awaitTermination()
  }
}
