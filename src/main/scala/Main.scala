import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc}

import scala.::

object Main {
  //Find out how many orders, how many products and how many sellers are in the data.
  //How many products have been sold at least once? Which is the product contained in more orders?
  def warmupFuncintion1(spark: SparkSession): Unit ={
    val dfProducts = spark.read.parquet("F:\\Bigdata_file\\products_parquet")
    val dfSellers = spark.read.parquet("F:\\Bigdata_file\\sellers_parquet")
    val dfSales = spark.read.parquet("F:\\Bigdata_file\\sales_parquet")
//    println("Number of products: ",dfProducts.count())
//    println("Number of sellers: ",dfSellers.count())
//    println("Number of orders: ",dfSales.count())
//    How many products have been sold at least once?
//    println("Number of products have been sold at least once: ",dfSales.select(col("product_id")).distinct().count())
    //Which is the product contained in more orders?
//    println("the product contained in more orders is: ",
////
////    )
//    val dfSaleTemp = dfSales.withColumn("num_pieces_sold_int",col("num_pieces_sold").cast("Integer"))
//    val df2 = dfSaleTemp.groupBy("product_id").agg(count("*").alias("cnt")).orderBy(col("cnt").desc).limit(1).show()
    //Which is the product contained in more orders?
//    println(df2.toString())


//    How many distinct products have been sold in each day?
//    dfSales.groupBy("date").agg(countDistinct("product_id").as("COunt distinct")).show(10,false)
    dfProducts.printSchema()
    dfSales.printSchema()
//    What is the average revenue of the orders?
  val dfjoinProductSale = dfSales.join(dfProducts,dfSales("product_id")===dfProducts("product_id"),"inner").agg(avg(dfSales("num_pieces_sold")*dfProducts("price"))).show()
  }
  def joinWithSaltKey(spark: SparkSession): Unit ={
    val dfProducts = spark.read.parquet("F:\\Bigdata_file\\products_parquet")
    val dfSellers = spark.read.parquet("F:\\Bigdata_file\\sellers_parquet")
    val dfSales = spark.read.parquet("F:\\Bigdata_file\\sales_parquet")
    //step 1: Check and select the skewed keys
    val result = dfSales.groupBy(dfSales("product_id")).count().sort(col("count").desc).limit(100).collect()
    var replicated_products :List[String] = List()
    var l :List[String] = List()
    val r = scala.util.Random
    val REPLICATION_FACTOR = 101
    for (r <- result){
      replicated_products:+=r(0).asInstanceOf[String]
      for(_rep <- 0 to REPLICATION_FACTOR){
        l:+=r(0).asInstanceOf[String] + _rep
        println(_rep)
      }
    }
    val rdd = spark.sparkContext.parallelize(l)
    val replicated_rdd = rdd.map(f=>(f(0), f(1)))
    replicated_rdd.foreach(f=>{
      println(f)
    })
//    val replicated_df = spark.createDataFrame(replicated_rdd).toDF("product_id","replication")
//    replicated_df.printSchema()
//    replicated_df.show(5,false)
//    println(l(1))
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.executor.memory", "500mb")
      .appName("QUANG")
      .getOrCreate()
//    warmupFuncintion1(spark)
    joinWithSaltKey(spark)
  }
}
