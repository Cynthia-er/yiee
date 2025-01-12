package cn.my.ml.bayes

import cn.my.commons.util.SparkUtil
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable

/**
 * 调用sparkmllib中现成的朴素贝叶斯算法，来进行出轨预测（分类）
 */
object NaiveBayesChugui {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName, "local[*]", Map.empty, ifHive = false)
    import org.apache.spark.sql.functions._
    import spark.implicits._
    /**
     * 模型训练
     */
    //加载训练数集
    val sample = spark.read.option("header", true).csv("userprofile/data/bayes.chugui/sample/sample.csv")

    val arr2vec: UserDefinedFunction = udf((arr:mutable.WrappedArray[String])=>{
      //用hash映射来生成特征向量
      val features = new Array[Double](20)
      arr.foreach(s=>{
        val idx = (s.hashCode & Integer.MAX_VALUE) % features.size
        features(idx) = 1.0
      })
      Vectors.dense(features)
    })
    spark.udf.register("arr2vec",arr2vec)

    //向量化 name,label,vec
    val sampleVecs: DataFrame = sample.selectExpr("name",
      "case label when '出轨' then 0.0 else 1.0 end as label",
      "arr2vec(array(job, income, age, sex)) as vec"
    )

    // 构造朴素贝叶斯算法工具
    val bayes = new NaiveBayes()
    .setFeaturesCol("vec")  // 特征向量
      .setLabelCol("label") // 标签列
      .setSmoothing(1.0) //拉普拉斯平滑系数

    // 用算法工具对训练集训练模型
    val model = bayes.fit(sampleVecs)

    //model.save("userprofile/data/bayes.chugui/model")

    /**
     * 1、利用模型做预测
     */
    //加载之前训练好的模型
    //val model1 = NaiveBayesModel.load("userprofile/data/bayes.chugui/model")
    val test = spark.read.option("header", true).csv("userprofile/data/bayes.chugui/test/test.csv")
    //对带预测数据集进行特征处理（特征工程）
    test.createTempView("test")
    val testVecs = spark.sql(
      """
        |select
        |name,
        |arr2vec(
        |  array(job,income,age,sex)
        |)as vec
        |From test
        |""".stripMargin
    )

    //用模型来预测
    val res = model.transform(testVecs)
    res.show(100,false)

    /**
     *   +----+---------------------------------------------------------------------------------+-----------------------------------------+----------------------------------------+----------+
     *   |name|vec                                                                              |rawPrediction                            |probability                             |prediction|
     *   +----+---------------------------------------------------------------------------------+-----------------------------------------+----------------------------------------+----------+
     *   |曹操|[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0]|[-10.049568976801629,-10.555940250218239]|[0.6239554317548746,0.3760445682451255] |0.0       |
     *   |小乔|[1.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0]|[-10.62493312170519,-10.961405358326404] |[0.5833333333333336,0.41666666666666646]|0.0       |
     *   |吕布|[0.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0]|[-10.519572606047365,-9.821971075138038] |[0.3323442136498511,0.6676557863501488] |1.0       |
     *   +----+---------------------------------------------------------------------------------+-----------------------------------------+----------------------------------------+----------+
     */

    spark.close()
  }
}
