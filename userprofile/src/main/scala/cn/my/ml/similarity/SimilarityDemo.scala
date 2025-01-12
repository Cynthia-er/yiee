package cn.my.ml.similarity

import cn.my.commons.util.SparkUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable


/**
 * 数捃集
 * id,f1,f2,f3,f4,f5
 * 1,40,50 10,2,5
 * 2,80,100,20,4,10
 * 3,121,148,28,6,15
 * 4,35,45,8,2,4
 * 5,70,92,16,4,7.9
 * 6,103,136,23,6.6,12.5
 *
 * 请计算:上面所有向量两两之间的余弦相似度，和欧式距离衡量的相似度
 **/
object SimilarityDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)

    val schema = new StructType()
      .add(name = "id", DataTypes.IntegerType)
      .add(name = "f1", DataTypes.DoubleType)
      .add(name = "f2", DataTypes.DoubleType)
      .add(name = "f3", DataTypes.DoubleType)
      .add(name = "f4", DataTypes.DoubleType)
      .add(name = "f5", DataTypes.DoubleType)

    val df = spark.read.schema(schema).option("header", true).csv("D:\\IdeaProjects\\yiee\\userprofile\\data\\similarity\\demo.csv")
    df.createTempView("df")

    //计算欧式距离相似度的函数
    val eud = (f1:mutable.WrappedArray[Double],f2:mutable.WrappedArray[Double])=>{
      // | 40.0 | 50.0 | 10.0 | 2.0 | 5.0 |
      // | 80.0 | 100.0 | 20.0 | 4.0 | 10.0 |
      // => [(40,80),(50,100),(10,20),(2,4),(5,10)]
      // => [1600,2500,100,4,25]
      val d2 = f1.zip(f2).map{tp=> Math.pow(tp._1-tp._2,2)}.sum
      1/(Math.pow(d2,0.5)+1)
    }

    //计算余弦相似度的函数
    val cos = (f1:mutable.WrappedArray[Double], f2:mutable.WrappedArray[Double])=>{
      val f1Mo = Math.pow(f1.map(Math.pow(_,2)).sum,0.5)
      val f2Mo = Math.pow(f2.map(Math.pow(_,2)).sum,0.5)
      val dj = f1.zip(f2).map{tp=>tp._1*tp._2}.sum

      dj/(f1Mo * f2Mo)
    }

    //将函数注册
    spark.udf.register("eud",eud)
    spark.udf.register("cos",cos)

    spark.sql(
      """
        |
        |select
        |a.id,
        |b.id,
        |eud(array(a.f1,a.f2,a.f3,a.f4,a.f5),array(b.f1,b.f2,b.f3,b.f4,b.f5)) as eud,
        |cos(array(a.f1,a.f2,a.f3,a.f4,a.f5),array(b.f1,b.f2,b.f3,b.f4,b.f5)) as cos
        |
        |from df a join df b on a.id < b.id
        |
        |""".stripMargin)
      .show(100,false)

    }
}
