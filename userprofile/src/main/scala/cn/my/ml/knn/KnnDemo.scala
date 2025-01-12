package cn.my.ml.knn

import cn.my.commons.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable

/**
 * KNN 算法，分类问题应用案例
 */
object KnnDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName, "local[*]", Map.empty, ifHive = false)

    // 设置日志级别为WARN
    Logger.getLogger("org").setLevel(Level.WARN)

    //加载样本集
    val schema1 = new StructType()
      .add(name = "label", DataTypes.DoubleType)
      .add(name = "f1", DataTypes.DoubleType)
      .add(name = "f2", DataTypes.DoubleType)
      .add(name = "f3", DataTypes.DoubleType)
      .add(name = "f4", DataTypes.DoubleType)
      .add(name = "f5", DataTypes.DoubleType)
    val sample = spark.read.schema(schema1).option("header", true).csv("userprofile/data/knn/sample.csv")
    sample.createTempView("sample")

    //加载测试集
    val schema2 = new StructType()
      .add(name = "id", DataTypes.DoubleType)
      .add(name = "f1", DataTypes.DoubleType)
      .add(name = "f2", DataTypes.DoubleType)
      .add(name = "f3", DataTypes.DoubleType)
      .add(name = "f4", DataTypes.DoubleType)
      .add(name = "f5", DataTypes.DoubleType)
    val test = spark.read.schema(schema2).option("header", true).csv("userprofile/data/knn/test.csv")
    test.createTempView("test")

    //计算欧式距离相似度的函数
    val eudi = (f1:mutable.WrappedArray[Double],f2:mutable.WrappedArray[Double])=>{
      // | 40.0 | 50.0 | 10.0 | 2.0 | 5.0 |
      // | 80.0 | 100.0 | 20.0 | 4.0 | 10.0 |
      // => [(40,80),(50,100),(10,20),(2,4),(5,10)]
      // => [1600,2500,100,4,25]
      val d2 = f1.zip(f2).map{tp=> Math.pow(tp._1-tp._2,2)}.sum
      1/(Math.pow(d2,0.5)+1)
    }
    spark.udf.register("eudi",eudi)

    /**
     * 1、拿这个未知点，和所有已知类别的样本点，逐一计算一次距离（相似度）
     * 具体做法：就是将测试集和样本进行笛卡尔积，join到一起，然后计算距离
     */
    val joined = spark.sql(
      """
        |select
        |a.id,
        |b.label,
        |eudi(array(a.f1,a.f2,a.f3,a.f4,a.f5),array(b.f1,b.f2,b.f3,b.f4,b.f5)) as eudi
        |from test a join sample b
        |""".stripMargin)

    /**
     * 2、根据距离排序，找到离这个未知点距离最近（相似度最大）的k个样本点
     */
    joined.createTempView("joined")
    val n5 = spark.sql(
      """
        |select id,
        |label
        |from
        |(
        |select
        |id,
        |label,
        |row_number() over(partition by id order by eudi desc)as rn
        |from joined
        |)o
        |where rn < 5
        |""".stripMargin
    )

    /**
     * 3、从这个距离最近的k个样本点中，判断哪一种类别的占比更大（这就是算法的输出结果）
     * +---+-----+
     * |id |label|
     * +---+-----+
     * |1.0|0.0  |
     * |1.0|0.0  |
     * |1.0|0.0  |
     * |1.0|0.0  |
     * |4.0|1.0  |
     * |4.0|1.0  |
     * |4.0|1.0  |
     * |4.0|1.0  |
     */
    n5.createTempView("n5")
    val result = spark.sql(
      """
        |select
        |id,
        |if(sum(label)>2,1,0) as predict
        |from n5
        |group by id
        |order by id
        |""".stripMargin
    ).show(100,false)
  }
}
