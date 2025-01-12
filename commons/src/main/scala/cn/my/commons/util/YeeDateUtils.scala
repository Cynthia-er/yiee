package cn.my.commons.util

import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}

import java.util.{Calendar, Date}
object YieeDateutils {
  def dateDiff(dt1:String,dt2:String):Long = {
    val st = DateUtils.parseDate(dt1, Array("yyyy-MM-dd"))
    val ed = DateUtils.parseDate(dt2, Array("yyyy-MM-dd"))
    val day1: Long = DateUtils.getFragmentInDays(st, Calendar.YEAR)
    val day2: Long = DateUtils.getFragmentInDays(ed, Calendar.YEAR)

    day2 - day1
  }
    def dateAdd(dt:String,days:Int):String = {
      val d = DateUtils.parseDate(dt, Array("yyyy-MM-dd"))
      val date: Date = DateUtils.addDays(d, days)

      DateFormatUtils.format(date, "yyyy-MM-dd")
    }
      def dateAdd2(dt:String,days:Int):Date ={
        val d= DateUtils.parseDate(dt,Array("yyyy-MM-dd"))
        DateUtils.addDays(d,days)

      }
}

