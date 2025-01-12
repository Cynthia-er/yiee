package cn.my.ml.bayes

import cn.my.commons.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 手写朴素贝叶斯算法实现
 * 利用算法来预测明星出轨概率
 */
object BayesChugui {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName, "local[*]", Map.empty, ifHive = false)
    // 设置日志级别为WARN
    Logger.getLogger("org").setLevel(Level.WARN)

    /**
     * 1、用样本集来训练模型
     * 各种特征值发生的 条件概率 和 独立概率
     * 还有：类别概率
     */
    //加载样本数据
    val sample = spark.read.option("header", true).csv("D:\\IdeaProjects\\yiee\\userprofile\\data\\bayes.chugui\\sample\\sample.csv")
    sample.cache()
    sample.createTempView("sample")

    //计算整个样本总条数
    val sample_cnt = sample.count()

    //计算类别条数
    val chugui_cnt = sample.where("label='出轨'").count()

    //计算类别发生概率
    val label_prob = spark.sql(
      """
          select
          label,
          count(1)/"""+sample_cnt+""" as label_prob  --每种类别发生的概率
          from sample
          group by label
          """.stripMargin)
//      .show(100,false)

    /**
     * +-----+------------------+
     * |label|label_prob        |
     * +-----+------------------+
     * |出轨 |0.4666666666666667|
     * |没出 |0.5333333333333333|
     * +-----+------------------+
     */

    /**
     * 计算各种特征，各种值的独立概率
     */
    //计算 job特征的各种值的独立概率 P(老师) P(程序员) P(公务员)
    val job_prob = spark.sql(
      """
          select
          job,
          count(1)/"""+sample_cnt+""" as feature_prob
          from sample
          group by job
          """.stripMargin)
//      .show(100,false)

    /**
     * +------+------------------+
     * |job   |feature_prob      |
     * +------+------------------+
     * |公务员|0.3333333333333333|
     * |老师  |0.3333333333333333|
     * |程序员|0.3333333333333333|
     * +------+------------------+
     */

    //计算 income特征的各种值的独立概率 P(高) P(中) P(低)
    val income_prob = spark.sql(
      """
          select
          income,
          count(1)/"""+sample_cnt+""" as feature_prob
          from sample
          group by income
          """.stripMargin)
//      .show(100,false)

    /**
     * +------+-------------------+
     * |income|feature_prob       |
     * +------+-------------------+
     * |中    |0.4                |
     * |低    |0.3333333333333333 |
     * |高    |0.26666666666666666|
     * +------+-------------------+
     */

    //计算 age 特征的各种值的独立概率 P(中年) P(青年) P(老年)
    val age_prob = spark.sql(
      """
          select
          age,
          count(1)/"""+sample_cnt+""" as feature_prob
          from sample
          group by age
          """.stripMargin)
//      .show(100,false)

    /**
     * +----+-------------------+
     * |age |feature_prob       |
     * +----+-------------------+
     * |老年|0.26666666666666666|
     * |青年|0.4                |
     * |中年|0.3333333333333333 |
     * +----+-------------------+
     */

    //计算 sex 特征的各种值的独立概率 P(男) P(女)
    val sex_prob = spark.sql(
      """
          select
          sex,
          count(1)/"""+sample_cnt+""" as feature_prob
          from sample
          group by sex
          """.stripMargin)
//      .show(100,false)

    /**
     * +---+------------------+
     * |sex|feature_prob      |
     * +---+------------------+
     * |男 |0.5333333333333333|
     * |女 |0.4666666666666667|
     * +---+------------------+
     */

    /**
     * 计算各种特征的各种值的条件概率
     */
    //计算职业特征各种值的条件概率
    val job_condition = spark.sql(
      """
          select
          job,
          count(if(label='出轨',1,null))/"""+chugui_cnt+""" as feature_prob
          from sample
          group by job
          """.stripMargin)
//      .show(100,false)

    /**
     * +------+-------------------+
     * |job   |job_condition_prob |
     * +------+-------------------+
     * |公务员|0.2857142857142857 |
     * |老师  |0.42857142857142855|
     * |程序员|0.2857142857142857 |
     * +------+-------------------+
     */

    //计算收入特征各种值的条件概率
    val income_condition = spark.sql(
      """
          select
          income,
          count(if(label='出轨',1,null))/"""+chugui_cnt+""" as feature_prob
          from sample
          group by income
          """.stripMargin)
//      .show(100,false)

    /**
     * +------+---------------------+
     * |income|income_condition_prob|
     * +------+---------------------+
     * |中    |0.42857142857142855  |
     * |低    |0.14285714285714285  |
     * |高    |0.42857142857142855  |
     * +------+---------------------+
     */

    //计算age特征各种值的条件概率
    val age_condition = spark.sql(
      """
          select
          age,
          count(if(label='出轨',1,null))/"""+chugui_cnt+""" as feature_prob
          from sample
          group by age
          """.stripMargin)
//      .show(100,false)

    /**
     * +----+-------------------+
     * |age |age_condition_prob |
     * +----+-------------------+
     * |老年|0.2857142857142857 |
     * |青年|0.42857142857142855|
     * |中年|0.2857142857142857 |
     * +----+-------------------+
     */

    //计算性别特征各种值的条件概率
    val sex_condition = spark.sql(
      """
          select
          sex,
          count(if(label='出轨',1,null))/"""+chugui_cnt+""" as feature_prob
          from sample
          group by sex
          """.stripMargin)
//      .show(100,false)

    /**
     * +---+-------------------+
     * |sex|sex_condition_prob |
     * +---+-------------------+
     * |男 |0.5714285714285714 |
     * |女 |0.42857142857142855|
     * +---+-------------------+
     */

    //将前面训练得到的模型数据，全部做成广播变量
    val lb_prob_map = label_prob.rdd.map(row => (row.getAs[String]("label"), row.getAs[Double]("label_prob"))).collectAsMap()
    val job_prob_map = job_prob.rdd.map(row => (row.getAs[String]("job"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val income_prob_map = income_prob.rdd.map(row => (row.getAs[String]("income"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val age_prob_map = age_prob.rdd.map(row => (row.getAs[String]("age"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val sex_prob_map = sex_prob.rdd.map(row => (row.getAs[String]("sex"), row.getAs[Double]("feature_prob"))).collectAsMap()

    val job_con_map = job_condition.rdd.map(row => (row.getAs[String]("job"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val income_con_map = income_condition.rdd.map(row => (row.getAs[String]("income"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val age_con_map = age_condition.rdd.map(row => (row.getAs[String]("age"), row.getAs[Double]("feature_prob"))).collectAsMap()
    val sex_con_map = sex_condition.rdd.map(row => (row.getAs[String]("sex"), row.getAs[Double]("feature_prob"))).collectAsMap()

    val bc_lb = spark.sparkContext.broadcast(lb_prob_map)
    val bc_job = spark.sparkContext.broadcast(job_prob_map)
    val bc_inc = spark.sparkContext.broadcast(income_prob_map)
    val bc_age = spark.sparkContext.broadcast(age_prob_map)
    val bc_sex = spark.sparkContext.broadcast(sex_prob_map)

    val bc_job_con = spark.sparkContext.broadcast(job_con_map)
    val bc_inc_con = spark.sparkContext.broadcast(income_con_map)
    val bc_age_con = spark.sparkContext.broadcast(age_con_map)
    val bc_sex_con = spark.sparkContext.broadcast(sex_con_map)

    import spark.implicits._
    /**
     * 2、利用前面训练好的模型，来对未知数据做预测
     */
    //加载测试数据
    val test = spark.read.option("header",true).csv("D:\\IdeaProjects\\yiee\\userprofile\\data\\bayes.chugui\\test\\test.csv")
    test.rdd.map({
      case Row(name:String,job:String,income:String,age:String,sex:String)
        =>{
        val lbprob = bc_lb.value

        val jobprob = bc_job.value
        val incprob = bc_inc.value
        val ageprob = bc_age.value
        val sexprob = bc_sex.value

        val jobcon = bc_job_con.value
        val inccon = bc_inc_con.value
        val agecon = bc_age_con.value
        val sexcon = bc_sex_con.value

        val p1: Double = lbprob.getOrElse("出轨", 1)
        val p2: Double = jobcon.getOrElse(job, 1)
        val p3: Double = inccon.getOrElse(income, 1)
        val p4: Double = agecon.getOrElse(age, 1)
        val p5: Double = sexcon.getOrElse(sex, 1)

        val p6: Double = jobprob.getOrElse(job,1)
        val p7: Double = incprob.getOrElse(income,1)
        val p8: Double = ageprob.getOrElse(age,1)
        val p9: Double = sexprob.getOrElse(sex,1)

        (name,(p1 * p2 * p3 * p4 * p5)/(p6 * p7 * p8 * p9))
      }
    }).toDF("name","probability")
      .show(100,false)

    /**
     * +----+-------------------+
     * |name|probability        |
     * +----+-------------------+
     * |曹操|0.6325489379425236 |
     * |小乔|0.5060391503540191 |
     * |吕布|0.19679300291545185|
     * +----+-------------------+
     */

    spark.close()
  }
}
