import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, lit, monotonically_increasing_id, rand, round, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, functions}

object DataSkewness {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.executor.memory", "500mb")
      .appName("QUANG")
      .getOrCreate()
    var users = Seq(("user1", "QUANG1"), ("user2", "QUANG2"), ("user3", "QUANG3"))
    val orders = Seq((1, "user1"), (2, "user2"), (3, "user3"), (4, "user1"), (5, "user1")
      , (6, "user1"), (7, "user1"))
    var users_df = spark.createDataFrame(spark.sparkContext.parallelize(users)).toDF("id", "value")
    var orders_df = spark.createDataFrame(spark.sparkContext.parallelize(orders)).toDF("order_id", "user_id")
    val result = orders_df.groupBy(col("user_id")).count().sort(col("count").desc).limit(10).collect()
    val REPLICATION_FACTOR = 2
    var l: List[(String, Int)] = List()
    var repli: List[Any] = List()
    for (r <- result) {
      repli :+= r(0)
      for (_rep <- 0 to REPLICATION_FACTOR) {
        l :+= (r(0).asInstanceOf[String], _rep)
      }
    }
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(l)
    val df_salt = spark.createDataFrame(rdd).toDF("user_id", "salt_id")
    users_df = users_df.join(broadcast(df_salt), users_df("id") === df_salt("user_id"), "left").
      withColumn("user_id_salt",when(df_salt("user_id").isNull,users_df("id")).otherwise( functions.concat(col("id"), col("salt_id")))).drop(col("user_id"))
    users_df.show(10, false)
    orders_df.printSchema()
    orders_df = orders_df.withColumn("user_id_salt", functions.when(col("user_id").isin(repli: _*),
      functions.concat(col("user_id"), round(rand() * REPLICATION_FACTOR, 0).cast(IntegerType))).otherwise(col("user_id")))
    orders_df.show(10, false)
    users_df.join(orders_df, orders_df("user_id_salt") === users_df("user_id_salt"), "inner").show(false)
  }
}
