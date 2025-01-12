package cn.my.dw.sql

import cn.my.commons.util.SparkUtil

import scala.collection.immutable

/**
 * 用户活跃度分析报表计算
 * 源表:用户连续活跃区间记录表
 * 目标:ADS_APL_UCA
 *
 * 连续活跃区间记录表说明如下：
 * CREATE TABLE DEMO_DWS_APL_UCA_RNG(
 * guid       string,  -- 用户唯一标识
 * first_dt   string,  --新增日期
 * rng_start  string,  --区间起始
 * rng_end    string   --区间结束
 * )
 * ROW FORMAT DELIMITED FIELDS TERMINATED BY',
 * ;
 * 假设今天为2020-06-08号
 * --导入数据
 * a,2020-05-20,2020-05-20,2020-05-26
 * a,2020-05-20,2020-05-29,2020-06-01
 * a,2020-05-20,2020-06-03,9999-12-31
 * b,2020-05-22,2020-05-22,2020-05-30
 * b,2020-05-22,2020-06-03,2020-06-05
 * t,2020-05-22,2020-05-22,2020-05-30
 * t,2020-05-22,2020-06-07,9999-12-31
 * c,2020-06-03,2020-06-03,9999-12-31
 * x,2020-06-04,2020-06-04,2020-06-05
 *
 * 计算，在当天的所有活跃用户中，
 * 连续活跃了1天的，?人
 * 连续活跃了2天的，?人
 * 连续活跃了3天的，?人
 * 连续活跃了4天的，?人
 * ......
 *
 * 逻辑:
 * 1.过滤出当天有活跃的记录 where rng_end='9999-12-31'
 * 2.根据这个用户的（区间起始 - 当天）天数差，生成多条数据: --》 guid 活跃天数
 * 3.根据活跃天数分组，count人数，即可得到各种活跃天数下的人数
 **/
object ADS_APL_UCA {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)

    //rdd与dataframe的转换
    import spark.implicits._

    //读取 用户连续活跃区间记录表数据
    //模拟：数据在一个csv文件中    真实：应该在hive表DWS_APL_UCA_RNG中
    val rngDF = spark.read.option("header",true).csv("data/active_range/rng.dat")

    //对数据过滤（当天活跃用户记录）
    val actDF = rngDF.where("rng_end = '9999-12-31'")

    /**
     * +----+----------+----------+----------+
     * |guid|first_dt  |rng_start |rng_end   |
     * +----+----------+----------+----------+
     * |a   |2020-05-20|2020-06-03|9999-12-31|  ==> [(a,1),(a,2),(a,3),...]
     * |t   |2020-05-22|2020-06-07|9999-12-31|  ==> []
     * |c   |2020-06-03|2020-06-03|9999-12-31|
     * +----+----------+----------+----------+
     */

   val diffDF = actDF.selectExpr("guid","datediff('2020-06-08',rng_start) as days")

    /**
     * +----+----+
     * |guid|days|
     * +----+----+
     * |   a|   5|
     * |   t|   1|
     * |   c|   5|
     * +----+----+
     */

    //根据 当日 - 区间起始 的差值，来生成多条 1~差值 的数据
    val actDaysDF = diffDF.rdd.flatMap(row => {
      val guid = row.getAs[String]("guid")
      val days = row.getAs[Int]("days")
      for (i <- 1 to days + 1) yield (guid, i)
    }).toDF("guid","act_days")
    /**
     * +----+--------+
     * |guid|act_days|
     * +----+--------+
     * |a   |1       |
     * |a   |2       |
     * |a   |3       |
     * |a   |4       |
     * |a   |5       |
     * |a   |6       |
     * |t   |1       |
     * |t   |2       |
     * |c   |1       |
     * |c   |2       |
     * |c   |3       |
     * |c   |4       |
     * |c   |5       |
     * |c   |6       |
     * +----+--------+
     */

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

    /**
     * +----------+--------+-----+
     * |        dt|act_days|users|
     * +----------+--------+-----+
     * |2020-06-08|       1|    3|
     * |2020-06-08|       2|    3|
     * |2020-06-08|       3|    2|
     * |2020-06-08|       4|    2|
     * |2020-06-08|       5|    2|
     * |2020-06-08|       6|    2|
     * +----------+--------+-----+
     */

    spark.close()

  }
}
