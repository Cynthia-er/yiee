package cn.my.commons.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSparkSession(appName:String = "app",master:String = "local[*]",confMap:Map[String,String]=Map.empty,ifHive:Boolean):SparkSession ={

    val conf = new SparkConf()
    conf.setAll(confMap)

    if(ifHive) {
      conf.set("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      SparkSession.builder()
        .appName(appName)
        .master(master)
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    }else{
      SparkSession.builder()
        .appName(appName)
        .master(master)
        .config(conf)
        .getOrCreate()
    }

  }

}
