import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast}

object Ex2 {
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
    //seller table is small, so we can broadcast
    print(dfSales.join(broadcast(dfSellers),dfSales("seller_id")===dfSellers("seller_id"),"inner").withColumn("ratio",
      dfSales("num_pieces_sold")/dfSellers("daily_target")).groupBy(dfSales("seller_id")).agg(avg("ratio")).show())
  }
}
