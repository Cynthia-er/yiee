package cn.my.ml.profile

import cn.my.commons.util.SparkUtil
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF}

import java.util
/**
 * 用户商品评论贝叶斯分类模型训练
 * 情感分类：  好评  中评  差评
 */
object CommentClassify {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, "local[*]", Map.empty, ifHive = false)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加载好评样本
    val good = spark.read.textFile("").selectExpr("value as cmt","cast(0.0 as double) as label")

    //加载中评样本
    val general = spark.read.textFile("").selectExpr("value as cmt","cast(1.0 as double) as label")

    //加载差评样本
    val poor = spark.read.textFile("").selectExpr("value as cmt","cast(2.0 as double) as label")

    //合并3类样本
    val sample = good.union(general).union(poor)   //结构：(cmt,label)

    //加载停止词字典
    val stopWordsSet = spark.read.textFile("userprofile/data/stopwords/stopwords.txt").rdd.collect().toSet
    val bc = spark.sparkContext.broadcast(stopWordsSet)

    //对中文评语进行分词处理
    val wordsSample = sample.map(row=>{

      val stopwords = bc.value

      //从row 中获取评论和标签
      val cmt = row.getAs[String]("cmt")
      val label = row.getAs[Double]("label")

      //对整条的评语分成词数组
      val terms: util.List[Term] = HanLP.segment(cmt)
      import scala.collection.JavaConversions._
      //将词列表转成字符串词数组，并过滤停止词
      val words = terms.map(_.word).toArray.filter(w => !stopwords.contains(w))

      (words,label)
    }).toDF("words","label")

    wordsSample.show(20,false)

    //将词特征向量化（要用到hash词映射算法，tf-idf特征值提取算法）
    val tf = new HashingTF()
      .setInputCol("words")  //输入列
      .setNumFeatures(100000)  //词特征向量长度（hash数组的长度）设置尽量大些，防止hash冲突 =>做成稀疏向量： (100000,Array(1,18,38,56,87,...),Array(1,2,3,8,19,10,...) （向量长度，位置，词频）
      .setOutputCol("tf_vec")
    //用tf算法，将词数组喂给tf算法，映射成tf值向量
    val tfVecs = tf.transform(wordsSample)  //结构：(words,label,tf_vec)

    //用idf算法，将上面tf特征向量集合变成TF-IDF特征值向量集合
    val idf = new IDF()
      .setInputCol("tf_vec") //输入列为词频值向量集合
      .setOutputCol("tf_idf_vec") //输出列为tf-idf值向量集合 （向量长度，位置，tf-idf值） tf-idf值 = 词频 * idf值 = 词频 * log(样本容量/出现过该词的样本数)
    //将tf特征向量集合喂给idf算法，得到idf值向量集合
    val idModel = idf.fit(tfVecs)
    val tfidVecs = idModel.transform(tfVecs)  //结构：(words,label,tf_vec,tf_idf_vec)

    //将整个样本集分为训练集80% 和 测试集20%
    val Array(train,test) = tfidVecs.randomSplit(Array(0.8,0.2))

    //训练朴素贝叶斯模型
    val bayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("tf_idf_vec")
      .setSmoothing(1.0)
      .setModelType("multinomial")
    val model = bayes.fit(train)

    //测试模型的预测效果
    val predict = model.transform(test)

    //统计预测准确率
    val total = predict.count()
    val correct = predict.where("label=prediction").count()

    println("准确率为：" + correct.toDouble / total)

      //保存模型
    model.save("userprofile/data/cmt_bayes_model")

    //保存模型
  }
}
