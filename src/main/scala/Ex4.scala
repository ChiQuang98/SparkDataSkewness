import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.functions.{col, count, udf}

import java.nio.charset.StandardCharsets

object Ex4 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("ex1")
      .getOrCreate()
    var dfSales = spark.read.parquet("D:\\DataExample\\sales_parquet")
    dfSales.show(5,false)
    val algori = (orderid:Int,bill_text:String)=>{
      var ret = bill_text
      if(orderid%2==0){
        val num_A = bill_text.count(_ =='A')
        for (c <- 0 to num_A){
          ret = DigestUtils.md5Hex(ret.getBytes(StandardCharsets.UTF_8))
        }
      } else {
        ret = DigestUtils.sha256Hex(ret.getBytes(StandardCharsets.UTF_8))
      }
      ret
    }
    val convertUDF = udf(algori)
    dfSales.withColumn("hashed_bill",convertUDF(col("order_id"),col("bill_raw_text"))).groupBy(col("hashed_bill")).agg(count("*").alias("cnt"))
      .where(col("cnt")>1).show()
  }
}
