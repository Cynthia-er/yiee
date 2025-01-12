package cn.my.dw.sql

import cn.my.commons.util.SparkUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 活跃用户留存分析报表计算
 * 目标表：ADS_APL_ART_REC 活跃用户留存记录表
 * CREATE TABLE ADS_APL_ART_REC(
 * dt         string,   --计算日期
 * act_dt     string,   --活跃日期
 * rt_days    int   ,   --留存天数
 * rt_users   int   ,   --留存人数
 * )
 * stored as parquet;
 *
 * 源表: DWS_APL_UCA_RNG 用户连续访间区间记录表
 */
object ADS_APL_ART_REC {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = true)

    import spark.implicits._

    //读取   源表：DWS_APL_UCA_RNG 用户连续访间区间记录表
    val rng = spark.read.table("databasename.dws_apl_uca_rng").where("dt='2020-03-14' and rng_end='9999-12-31'")

    //展开区间
    val actDt = rng.map(row=>{
      val guid = row.getAs[Long]("guid")
      val rng_start = row.getAs[String]("rng_start")
      //比如，今天是3-14号，而rng_start = 3-10，那么，今天到3-10之间，一共有4天，所以，需要生成4条记录
      /**
       * 3-10
       * 3-11
       * 3-12
       * 3-13
       */
      import cn.my.commons.util.YieeDateutils.{dateAdd, dateDiff}
      val diff: Long = dateDiff(rng_start,"2020-03-14")
      for(i <- 0 to diff.toInt) yield (guid,dateAdd(rng_start,i.toInt))
    }).toDF("guid","act_dt")

    //汇总聚合
    actDt.createTempView("act")
    val res = spark.sql(
      """
        |select
        |'2020-03-14' as dt,
        |act_dt,
        |datediff('2020-03-14',act_dt) as rt_days,
        |count(1) as rt_users
        |from act
        |group by act_dt
        |""".stripMargin
    )

    res.write.mode(SaveMode.Append).saveAsTable("ADS_APL_ART_REC")

    spark.close()

  }
}
