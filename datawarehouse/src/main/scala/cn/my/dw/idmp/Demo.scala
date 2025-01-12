package cn.my.dw.idmp

import cn.my.commons.util.SparkUtil
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
 * 为了做idmapping来写的一个图计算api使用demo
 * 找出哪些标识是同一个人
 * 13866778899,刘德华，wx_hz,2000
 * 13877669988,华仔，wx_hz,3000
 * 刘德华,wx_ldh,5000
 * 13912344321,马德华，wx_mdh,12000
 * 13912344321,二师兄,wx_bj,3500
 * 13912664321,猪八戒,wx_bj,5600
 */
object Demo {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)
    import spark.implicits._
    //加载原始数据
    val ds: Dataset[String] = spark.read.textFile("D:\\IdeaProjects\\yiee\\data\\graphx\\input\\dehua.txt")

    //构造一个点RDD
    val vertices: RDD[(Long, String)] = ds.rdd.flatMap(line => {
      val fields = line.split(",")

      for(ele <- fields if StringUtils.isNotBlank(ele)) yield (ele.hashCode.toLong,ele)

      //在spark的图计算api中，点需要表示成一个tuple ==> (点的唯一标识Long，点的数据)
//      Array((fields(0).hashCode.toLong, fields(0)),
//        (fields(1).hashCode.toLong, fields(1)),
//        (fields(2).hashCode.toLong, fields(2))
//      )

    })

    //构造一个边RDD
    //spark graphx中对边的描述结构：  Edge(起始点id,目标点id,边数据)
    val edges: RDD[Edge[String]] = ds.rdd.flatMap(line => {
      val fields = line.split(",")

//      val lst = new ListBuffer[Edge[String]]()
//      for (i <- 0 to fields.length - 2) {
//        val edge1 = Edge(fields(i).hashCode.toLong, fields(i + 1).hashCode.toLong, "")
//
//        lst += (edge1)
//      }
//      lst

      for(i <- 0 to fields.length - 2 if StringUtils.isNotBlank(fields(i))) yield Edge(fields(i).hashCode.toLong, fields(i + 1).hashCode.toLong, "")

    })

    //用 点集合 和 边集合 构造一张图
    val graph = Graph(vertices, edges)

    //调用图的算法：连通子图算法，得到一张图
    val graph2 = graph.connectedComponents()
    //从结果图中，取出图的点集合，即可以得到我们想要的分组结果
    val vertices2: VertexRDD[VertexId] = graph2.vertices
    //(点id-0,点数据-0)(点数据取的是最小的点id)
    //(点id-1,点数据-0)
    //(点id-4,点数据-4)
    //(点id-5,点数据-4)

//    vertices2.take(30).foreach(println)

    //将上面得到的映射关系rdd，收集到Driver端，然后作为变量广播出去
    val idmpMap = vertices2.collectAsMap()
    val bc = spark.sparkContext.broadcast(idmpMap)

    //利用这个映射关系结果，来加工我们的原始数据
    val res = ds.map(line => {
      val bc_map = bc.value
      val name = line.split(",").filter(StringUtils.isNotBlank(_))(0)
      val gid = bc_map.get(name.hashCode.toLong).get
      gid + "," + line
    })

    res.show(10,false)

    spark.close()
  }

}
