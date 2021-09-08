import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, lit, sum}

object Ex3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("ex1")
      .getOrCreate()
    var dfProducts = spark.read.parquet("D:\\DataExample\\products_parquet")
    var dfSellers = spark.read.parquet("D:\\DataExample\\sellers_parquet")
    var dfSales = spark.read.parquet("D:\\DataExample\\sales_parquet")
    dfProducts.show(5,false)
    dfSellers.show(5,false)
    dfSales.show(5,false)
    dfSales = dfSales.groupBy(col("product_id"),col("seller_id")).agg(sum("num_pieces_sold").alias("num_pieces_sold"))
    dfSales.show(5,false)
    var window_desc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").desc)
    var window_asc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").asc)
    dfSales = dfSales.withColumn("rank_asc",dense_rank().over(window_asc)).withColumn("rank_desc",dense_rank().over(window_desc))
    val single_seller = dfSales.where(col("rank_asc")===col("rank_desc")).select(
      col("product_id").alias("single_seller_product_id"),col("seller_id").alias("single_seller_seller_id"), lit("Only seller or multiple sellers with the same results").alias("type")
    )
    val second_seller = dfSales.where(col("rank_desc")===2).select(
      col("product_id").alias("second_seller_product_id"), col("seller_id").alias("second_seller_seller_id"),
      lit("Second top seller").alias("type")
    )
    val least_seller = dfSales.where(col("rank_asc")===1)


  }
}
