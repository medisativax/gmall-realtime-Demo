package com.zhanglei.gmall.realtime.app.func

import com.alibaba.fastjson.JSONObject

trait DimJoinFunction[T] {
  def getkey(input: T): String
  def join(input: T, dimInfo: JSONObject): Unit
}
