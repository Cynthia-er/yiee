
package cn.my.dw.sql

import cn.my.commons.util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
 * 用户活跃度分析报表计算
 * 源表:用户连续活跃区间记录表
 * 目标:ADS_APL_UCA
 *
 * ...
 **/
object ADS_APL_UCA_ive {

  def main(args: Array[String]): Unit = {
    // 获取 Spark 会话并启用 Hive 支持
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = true)

    // 导入隐式转换
    import spark.implicits._

    // 从 Hive 表中读取数据
    val rngDF = spark.read.table("database_name.DWS_APL_UCA_RNG")

    // 对数据过滤（当天活跃用户记录）
    val actDF = rngDF.where("rng_end = '9999-12-31'")

    // 计算每个用户的活跃天数
    val diffDF = actDF.selectExpr("guid", "datediff('2020-06-08', rng_start) as days")

    // 根据当日 - 区间起始 的差值，生成多条 1~差值 的数据
    val actDaysDF = diffDF.rdd.flatMap(row => {
      val guid = row.getAs[String]("guid")
      val days = row.getAs[Int]("days")
      for (i <- 1 to days + 1) yield (guid, i)
    }).toDF("guid", "act_days")

    //按活跃天数分组聚合人数
    actDaysDF.createTempView("act")
    val resDF = spark.sql(
      """
        |select
        |   '2020-06-08' as dt,
        |   act_days,
        |   count(guid) as users
        |from act
        |group by act_days
        |order by act_days
        |""".stripMargin)

    resDF.show()

    // 将结果写入 Hive 表
    resDF.write.mode(SaveMode.Append).saveAsTable("database_name.ADS_APL_UCA")

    spark.stop()
  }
}