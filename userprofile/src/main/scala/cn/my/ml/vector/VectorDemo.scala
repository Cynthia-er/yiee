package cn.my.ml.vector

import cn.my.commons.util.SparkUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable

/**
 * 构造向量
 */
object VectorDemo {
  def main(args: Array[String]): Unit = {

    //mllib中所有数据都必须是double类型

    //构造一个密集型向量
    val vec1 = Vectors.dense(Array(18.0, 170, 175, 67, 1000))
    println(vec1)

    //构造一个稀疏性向量
    val vec2 = Vectors.sparse(10, Array(2, 5), Array(18, 66))
    println(vec2)

    //Vectors工具类上有求俩向量之间欧氏距离的工具方法
    val v1 = Vectors.dense(Array(1.0, 1.0))
    val v2 = Vectors.dense(Array(4.0, 5.0))
    val distance = Vectors.sqdist(v1, v2)
    println(distance)

    //将 出轨调查数据 加载成dataframe，并且其中的特征值组成Vector类型字段
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName, "local[*]", Map.empty, ifHive = false)
    val df = spark.read.option("header", true).csv("D:\\IdeaProjects\\yiee\\userprofile\\data\\bayes.chugui\\sample\\sample.csv")

    //schema:  name,label,vec
    val df2 = df.selectExpr("name",
    "case label when '出轨' then 0.0 else 1.0 end as label",
    "case job when '老师' then 0.0 when '程序员' then 1.0 else 2.0 end as job",
    "case income when '低' then 0.0 when '中' then 1.0 else 2.0 end as income",
    "case age when '青年' then 0.0 when '中年' then 1.0 else 2.0 end as age",
    "case sex when '男' then 0.0 when '女' then 1.0 end as sex"
    )

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val arr2vec: UserDefinedFunction = udf((arr:mutable.WrappedArray[Double])=>{Vectors.dense(arr.toArray)})

    //调api的方式来写sql
    val res = df2.select('name, 'label, arr2vec(array('job.cast(DataTypes.DoubleType), 'income.cast(DataTypes.DoubleType), 'age.cast(DataTypes.DoubleType), 'sex.cast(DataTypes.DoubleType))).as("vec"))
    //等价于 df2.selectExpr("name","label","arr2vec(array(cast(job as double),cast(income as double),cast(age as double),cast(sex as double)))")

    res.printSchema()

    /**
     * root
     * |-- name: string (nullable = true)
     * |-- label: decimal(2,1) (nullable = false)
     * |-- vec: vector (nullable = true)
     */
    res.show(100)

    /**
     * +------+-----+-----------------+
     * |  name|label|              vec|
     * +------+-----+-----------------+
     * |  张飞|  0.0|[0.0,1.0,0.0,0.0]|
     * |  赵云|  0.0|[0.0,1.0,1.0,1.0]|
     * |陆小凤|  1.0|[0.0,0.0,0.0,0.0]|
     * |花满楼|  0.0|[0.0,2.0,2.0,1.0]|
     * |  田汉|  1.0|[0.0,0.0,0.0,1.0]|
     * |  唐嫣|  1.0|[1.0,2.0,0.0,0.0]|
     * |刘亦菲|  0.0|[1.0,2.0,0.0,1.0]|
     * |汪小敏|  1.0|[1.0,1.0,1.0,0.0]|
     * |刘晓庆|  1.0|[1.0,1.0,1.0,0.0]|
     * |任我行|  0.0|[1.0,1.0,2.0,0.0]|
     * |  郭靖|  1.0|[2.0,1.0,2.0,1.0]|
     * |  黄蓉|  1.0|[2.0,0.0,2.0,1.0]|
     * |段正淳|  0.0|[2.0,2.0,1.0,0.0]|
     * |  段誉|  1.0|[2.0,0.0,1.0,1.0]|
     * |  虚竹|  0.0|[2.0,0.0,0.0,0.0]|
     * +------+-----+-----------------+
     */

    spark.close()
  }
}
