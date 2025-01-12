package cn.my.ml.HanLpDemo

import java.util
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.py.Pinyin
import com.hankcs.hanlp.seg.common.Term
/**
 * 中文分词工具包HanLp  api演示
 */
object HanLpDemo {
  def main(args: Array[String]): Unit = {

    val s = "我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集"

    val terms: util.List[Term] = HanLP.segment(s)
    import scala.collection.JavaConversions._
    for(t <- terms){
      println(t.word)
    }

    val pinyins: util.List[Pinyin] = HanLP.convertToPinyinList(s)
    pinyins.foreach(pinyin => println(pinyin.getPinyinWithoutTone))
  }
}
