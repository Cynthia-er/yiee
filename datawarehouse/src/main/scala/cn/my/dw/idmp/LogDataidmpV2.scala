package cn.my.dw.idmp

import cn.my.commons.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}


/**三类埋点日志的id映射计算程序
 * 考虑滚动整合,即考虑上一日的idmp字典整合的idmapping程序
 */

object LogDataidmpV2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)

    //一、加载3类数据
    val appLog: Dataset[String] = spark.read.textFile("\\app")
    val webLog = spark.read.textFile("\\web")
    val wxAppLog = spark.read.textFile("\\wxapp")

    //二、提取每一类数据中每一行的标识字段
    val app_ids: RDD[Array[String]] = extractIds(appLog)
    val web_ids: RDD[Array[String]] = extractIds(webLog)
    val wxapp_ids: RDD[Array[String]] = extractIds(wxAppLog)

    val ids = app_ids.union(web_ids).union(wxapp_ids)

    //三、构造图计算中的veritex集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      for (biaoshi <- arr) yield (biaoshi.hashCode.toLong, biaoshi)
    })

    //四、构造图计算中的Edge集合
    // [a,b,c,d] ==> a-b a-c a-d b-c b-d c-d
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
        //用双层for循环，来对一个数组中所有的标识进行两两组合成边
        for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
      })
      //将边变成（边,1）来计算一个边出现的次数
      .map(edge => (edge, 1))
      .reduceByKey(_ + _)
      //过滤掉出现次数小于经验阈值的边
      .filter(tp => tp._2 > 2)
      .map(tp => tp._1)

    //五、将上一日的idmap映射字典，解析成点、边集合
    val preDayIdmp = spark.read.parquet("data/idmp/2024-01-11")
    //构造点集合
    val preDayIdmpVertices = preDayIdmp.rdd.map({
      case Row(idFlag: VertexId, guid: VertexId) =>
        (idFlag, "")
    })
    //构造边集合
    val preDayEdges = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      Edge(idFlag, guid, "")
    })

    //六、将当日的点集合union上日的点集合，当日的边集合union上日的边集合,构造图，并调用最大连通子图算法
    val graph = Graph(vertices.union(preDayIdmpVertices),edges.union(preDayEdges))

    //VertexRDD[VertexId] ==> RDD[(点id:long,标识id(组中的最小值):long)]
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //八、将结果跟上日的映射字典做对比，调整guid
    //1、将上日的idmp映射结果字典收集到driver端，并广播
    val idMap = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      (idFlag, guid)
    }).collectAsMap()
    val bc = spark.sparkContext.broadcast(idMap)

    //2、将今日的图计算结果按照guid分组，然后去跟上日的映射字典进行对比
    val todayIdmpResult = res_tuples.map(tp=>(tp._2,tp._1))
      .groupByKey()
      .mapPartitions(iter=>{
        //从广播变量中取出上日的idmp映射字典
        val idmpMap = bc.value

        iter.map(tp=>{
          //当日的guid计算结果
          var todayGuid = tp._1
          //这一组中的所有id标识
          val ids = tp._2.toSet

          //遍历这一组id，挨个去上日的idmp映射字典中查找
          var find = false
          for(elem <- ids if !find){
            val maybeGuid: Option[Long] = idmpMap.get(elem)
            //如果这个id在昨日的映射字典中找到了，那么就用昨天的guid替换掉今天这一组的guid
            if(maybeGuid.isDefined) {
              todayGuid = maybeGuid.get
              find = true
            }
          }
          (todayGuid,ids)
        })
      })
      .flatMap(tp=>{
        val ids = tp._2
        val guid = tp._1
        for(elem <- ids) yield (elem,guid)    //最终结果：(elem,guid)
      })

    //可以直接用图计算所产生的结果中的组最小值，作为这一组的guid（当然，也可以自己另外生成一个UUID来作为GUID）
    import spark.implicits._
    //保存结果
    todayIdmpResult.coalesce(1).toDF("biaoshi_hashcode","guid").write.parquet("data/idmp/2024-01-12")

    spark.close()

  }

  /**
   * 从一个日志ds中提取各类标识id
   * @param logDs
   * @return
   */
  def extractIds(logDs:Dataset[String]): RDD[Array[String]] = {

    logDs.rdd.map(line=>{

      //将每一行数据解析成json对象
      val jsonObj = JSON.parseObject(line)

      //从json对象中取user对象
      val userObj = jsonObj.getJSONObject("user")
      val uid = userObj.getString("uid")

      //从user对象中取phone对象
      val phoneObj = userObj.getJSONObject("phone")
      val imei = userObj.getString("imei")
      val mac = userObj.getString("mac")
      val imsi = userObj.getString("imsi")
      val androidId = userObj.getString("androidId")
      val deviceId = userObj.getString("deviceId")
      val uuid = userObj.getString("uuid")

      Array(uid,imei,mac,imsi,androidId,deviceId,uuid).filter(StringUtils.isNotBlank(_))
    })
  }

}
