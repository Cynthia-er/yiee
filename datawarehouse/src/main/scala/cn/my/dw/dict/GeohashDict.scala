package cn.my.dw.dict

import ch.hsr.geohash.GeoHash
import cn.my.commons.util.SparkUtil
import org.apache.spark.sql.SparkSession

import java.util.Properties

object GeohashDict {
  def main(args: Array[String]): Unit = {

    //构造spark
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    //提取mysql中的gps左表地理位置表
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")

    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/dict", "tmp1", props)

    //将gps左表通过geohash算法转成geohash编码
    val res = df.map(row => {
      // 取出这一行的经、纬度
      val lng = row.getAs[Double](fieldName = "BD09_LNG")
      val lat = row.getAs[Double](fieldName = "BD09_LAT")
      val province = row.getAs[String](fieldName = "province")
      val city = row.getAs[String](fieldName = "city")
      val district = row.getAs[String](fieldName = "district")

      //调用geohash算法，得出geohash编码
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)

      // 组装返回结果
      (geoCode, province, city, district)
    }).toDF("geo","province","city","district")

    //保存结果
    res.write.parquet("data/dict/geo_dict/output")
    //feature1feature2feature3
    spark.close()
  }
}
