package cn.my.dw.pre

import ch.hsr.geohash.GeoHash
import cn.my.commons.util.SparkUtil
import cn.my.dw.beans.AppLogBean
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row}

import java.util
import scala.collection.convert.ImplicitConversions.`map AsScala`

/**
 * app埋点日志预处理
 */
object AppLogDataPreprocess {
  def main(args: Array[String]): Unit = {

    val session = SparkUtil.getSparkSession(this.getClass.getSimpleName,"local[*]",Map.empty,ifHive = false)
    import session.implicits._

    //加载当日的app埋点日志数据
//    val ds: Dataset[String] = session.read.textFile("\\app")
    val ds: Dataset[String] = session.read.textFile(args(0))

    //加载geo地理位置字典，并收集到driverr端，然后广播出去
//    val geoMap = session.read.parquet("data/dict/geo_dict/output")
    val geoMap = session.read.parquet(args(1))
      .rdd
      .map({
        case Row(geo:String,province:String,city:String,district:String)
          =>
          (geo,(province,city,district))
      })
      .collectAsMap()
    val bc_geo = session.sparkContext.broadcast(geoMap)

    //加载idmp映射字典，并收集到driver端，然后广播出去
//    val idmpMap = session.read.parquet("data/idmp/2024-01-12")
    val idmpMap = session.read.parquet(args(2))
      .rdd
      .map({
        case Row(biaoshi_hashcode: Long, guid: Long)
        => (biaoshi_hashcode, guid)
      })
      .collectAsMap()
    val bc_idmp = session.sparkContext.broadcast(idmpMap)

    //解析json
    val res=ds.map(line=>{
      try {
        val jsonobj = JSON.parseObject(line)
        //解析，抽取各个字段
        import scala.collection.JavaConverters._  //将java集合转成scala集合
        val eventid = jsonobj.getString("eventid")
        val event: Map[String, String] = jsonobj.getJSONObject("event").getInnerMap.asInstanceOf[util.Map[String,String]].toMap

        //返回结果：如果数据符合要求，则返回一个AppLogBean；如果不符合要求，就返回一个null

        val userobj = jsonobj.getJSONObject("user")
        val uid = userobj.getString("uid")

        val phoneobj = userobj.getJSONObject("phone")
        val imei = phoneobj.getString("imei")
        val imsi = phoneobj.getString("imsi")
        val mac = phoneobj.getString("mac")
        val osName = phoneobj.getString("osName")
        val osVer = phoneobj.getString("osVer")
        val androidId = phoneobj.getString("androidId")
        val resolution = phoneobj.getString("resolution")
        val deviceType = phoneobj.getString("deviceType")
        val deviceId = phoneobj.getString("deviceId")
        val uuid = phoneobj.getString("uuid")

        val appobj = userobj.getJSONObject("app")
        val appid = appobj.getString("appid")
        val appVer = appobj.getString("appVer")
        val release_ch = appobj.getString("release_ch")
        val promotion_ch = appobj.getString("promotion_ch")

        val locobj = userobj.getJSONObject("loc")
        val longtitude = locobj.getDouble("longtitude")
        val latitude = locobj.getDouble("latitude")
        val carrier = locobj.getString("carrier")
        val ip = locobj.getString("ip")
        val cid_sn = locobj.getString("cid_sn")
        val netType = locobj.getString("netType")

        val sessionId = userobj.getString("sessionId")

        val timestamp = jsonobj.getString("timestamp").toLong

        //判断数据是否符合规则
        //uid | imei | uuid | mac | androidId | imsi 标识字段不能全为空
        val sb = new StringBuilder
        val flagFields = sb.append(uid).append(imei).append(uuid).append(mac).append(androidId).append(imsi).toString().replaceAll("null", "")

        //event/eventid/sessionid 缺任何一个都不行
        var bean:AppLogBean = null
        if(StringUtils.isNotBlank(flagFields) && event != null && StringUtils.isNotBlank(eventid) && StringUtils.isNotBlank(sessionId)){

          //返回一个正常的case class对象
          bean = AppLogBean(
              Long.MinValue,
              eventid,
              event,
              uid,
              imei,
              mac,
              imsi,
              osName,
              osVer,
              androidId,
              resolution,
              deviceType,
              deviceId,
              uuid,
              appid,
              appVer,
              release_ch,
              promotion_ch,
              longtitude,
              latitude,
              carrier,
              netType,
              cid_sn,
              ip,
              sessionId,
              timestamp)
        }
         bean
      } catch {
        case e:Exception => null
      }
    })
      //过滤到不符合要求的数据
      .filter(_ != null)
      .map(bean=>{

        //取出广播变量中的字典
        val geoDict: collection.Map[String, (String, String, String)] = bc_geo.value
        val idmpDict: collection.Map[Long, Long] = bc_idmp.value

        //数据集成--省市区
        val lng = bean.longtitude
        val lat = bean.latitude
        val geo = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
        val maybeTuple = geoDict.get(geo)
        if(maybeTuple.isDefined){
          val area: (String, String, String) = maybeTuple.get
          bean.province = area._1
          bean.city = area._2
          bean.district = area._3
        }

        //数据集成--guid
        val ids = Array(bean.imei, bean.imsi, bean.mac, bean.uid, bean.androidId, bean.uuid)
        var find = false
        for (elem <- ids if !find) {
          val maybeLong = idmpDict.get(elem.hashCode.toLong)
          if(maybeLong.isDefined){
            bean.guid = maybeLong.get
            find = true
          }
        }
        bean
      })
      .filter(bean=> (bean.guid != Long.MinValue) )
      .toDF()
      .where("province != '未知'")
      .write
      .parquet(args(3))
//      .parquet("data/applog_processed/2024-01-12")

    session.close()


  }
}
