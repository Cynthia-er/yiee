package cn.my.dw.sql

import cn.my.commons.util.YieeDateutils.dateDiff
import cn.my.commons.util.{SparkUtil, YieeDateutils}
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.text.SimpleDateFormat
import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
 * 计算目标: 访问间隔分布用户聚合表DWS_APL_ITV_AGU
 * 截止日期       guid       问隔天数       发生次数
 * 2020-03-12     1           0            10
 * 2020-03-12     1           1            2
 * 2020-03-12     1           2            4
 *......
 *
 *计算源表: 用户连续活跃区间记录表  DWS_APL_UCA_RNG
 */
object DWS_APL_ITV_AGU {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)

    // 导入隐式转换
    import spark.implicits._

    //读取hive中的当日区间记录表
    //val rng = spark.read.table("database_name.DWS_APL_UCA_RNG")

    val schema =new StructType()
      .add( name ="guid",DataTypes.LongType)
      .add( name ="first_dt",DataTypes.StringType)
      .add( name ="rng_start",DataTypes.StringType)
      .add(name ="rng_end",DataTypes.StringType)
    val rng = spark.read.schema(schema).csv( path = "data/interval_demodata/demo.dat")

    /**
     * 1,2020-05-20,2020-01-01,2020-01-12
     * 1,2020-05-20,2020-02-09,2020-03-11
     * 1,2020-05-20,2020-03-13,9999-12-31
     * 2,2020-05-22,2020-03-02,2020-03-02
     * 2,2020-05-22,2020-03-04,2020-03-05
     * 3,2020-05-22,2020-03-02,2020-03-09
     * 3,2020-05-22,2020-03-12,9999-12-31
     * 4,2020-06-03,2020-03-04,9999-12-31
     * 5,2020-06-04,2020-03-12,2020-03-13
     */
    //对数据按最近30天的约束，做一个截断! 并且将9999-12-31换成当天日期，便于求差值
    rng.createTempView("rng")
    val rng2 = spark.sql(
      """
        |select
        |guid,
        |if(datediff('2020-03-14',rng_start)>30,date_sub('2020-03-14',30),rng_start) as rng_start,
        |if(rng_end='9999-12-31','2020-03-14',rng_end) as rng_end
        |from rng
        |where datediff('2020-03-14',rng_end)<=30
        |""".stripMargin)


    //rng2.show(100,false)
    /**
     * +----+----------+----------+
     * |guid|rng_start |rng_end   |
     * +----+----------+----------+
     * |1   |2020-02-13|2020-03-11|
     * |1   |2020-03-13|2020-03-14|
     * |2   |2020-03-02|2020-03-02|
     * |2   |2020-03-04|2020-03-05|
     * |3   |2020-03-02|2020-03-09|
     * |3   |2020-03-12|2020-03-14|
     * |4   |2020-03-04|2020-03-14|
     * |5   |2020-03-12|2020-03-13|
     * +----+----------+----------+
     */

    // 计算隔0天的发生次数，隔x天发生1次
    val rdd =rng2.rdd.map(row=> {
      val guid = row.getAs[Long](fieldName = "guid")
      val rng_start = row.getAs[String](fieldName = "rng_start")
      val rng_end = row.getAs[String](fieldName = "rng_end")
      (guid, (rng_start, rng_end))
    })
    //按guid分组 ==>(1,Iterator[(2020-02-13,2020-03-11),(2020-03-13,2020-03-14)])
    val rdd2 = rdd.groupByKey()

    // 将每个人的每个区间，去生成        (guid,隔 x 天,发生 y 次)
    val rdd3 = rdd2.flatMap(tp=> {
      val guid = tp._1
      val rngs: List[(String, String)] = tp._2.toList

      //隔0天的计算
      val itr0Day: List[(Long, Int, Int)] = rngs.map(rng => (guid, 0, dateDiff(rng._1, rng._2).toInt))

      //隔n天的计算  先将区间排序
      val sortedRngs: List[(String, String)] = rngs.sortBy(rng => rng._1)

      //迭代区间列表，错位求差值得到隔x天，1次
      val itrxDay: immutable.IndexedSeq[(Long, Int, Int)] = for(i <- 0 until sortedRngs.size-1) yield (guid,dateDiff(sortedRngs(i)._2, sortedRngs(i+1)._1).toInt,1)

      //返回
      itr0Day ++ itrxDay
    })

    //rdd3.take(100).foreach(println)
    /**
     * (4,0,10)
     * (1,0,27)
     * (1,0,1)
     * (1,2,1)
     * (3,0,7)
     * (3,0,2)
     * (3,3,1)
     * (5,0,1)
     * (2,0,0)
     * (2,0,1)
     * (2,2,1)
     */

    val res = rdd3.toDF("guid","itv_days","cnts").groupBy("guid","itv_days").sum("cnts").show(100,false)

    /**
     * +----+--------+---------+
     * |guid|itv_days|sum(cnts)|
     * +----+--------+---------+
     * |5   |0       |1        |
     * |3   |0       |9        |
     * |2   |0       |1        |
     * |3   |3       |1        |
     * |1   |2       |1        |
     * |2   |2       |1        |
     * |1   |0       |28       |
     * |4   |0       |10       |
     * +----+--------+---------+
     */

    //TODO 将res保存到hive即可

    spark.close()
  }
}
