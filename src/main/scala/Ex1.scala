import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast, col, concat, rand, round, when}
import org.apache.spark.sql.types.IntegerType

object Ex1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("ex1")
      .getOrCreate()
    var dfProducts = spark.read.parquet("D:\\DataExample\\products_parquet")
    var dfSellers = spark.read.parquet("D:\\DataExample\\sellers_parquet")
    var dfSales = spark.read.parquet("D:\\DataExample\\sales_parquet")
    dfSales.show(5,false)
    dfProducts.show(5,false)
    var result =  dfSales.groupBy(dfSales("product_id")).count().sort(col("count").desc).limit(100).collect()
    val REPLICATION_FACTOR = 100
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
    val df_salt = spark.createDataFrame(rdd).toDF("product_id","salt_id")
    df_salt.show(5,false)
    dfProducts = dfProducts.join(broadcast(df_salt),dfProducts("product_id")===df_salt("product_id"),"left")
      .withColumn("product_id_salt",when(df_salt("product_id").isNull,dfProducts("product_id"))
        .otherwise(concat(dfProducts("product_id"),df_salt("salt_id"))))
    dfSales = dfSales.withColumn("product_id_salt",when(dfSales("product_id").isin(repli:_*),
      concat(dfSales("product_id"),round(rand()*REPLICATION_FACTOR,0).cast(IntegerType))).otherwise(dfSales("product_id")))
    dfSales.show(5,false)
    println(dfSales.join(dfProducts,dfSales("product_id_salt")===dfProducts("product_id_salt"),"inner").agg(
      avg(dfProducts("price")*dfSales("num_pieces_sold"))).show())
  }
}
