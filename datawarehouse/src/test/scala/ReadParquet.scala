import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

object ReadParquet {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.parquet("data/dict/geo_dict/output")

//    df.show(10,false)

    val str1 = DigestUtils.md5Hex("dwoiawdx782cdse")
    val str2 = DigestUtils.md5Hex("jio7932kawdx782")
    println(str1)
    println(str2)

    spark.close()

  }
}
