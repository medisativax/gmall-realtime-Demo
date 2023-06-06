package com.zhanglei.gmall.realtime.app.func


import com.zhanglei.gmall.realtime.util.KeywordUtil
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

import java.io.IOException

@FunctionHint(output = new DataTypeHint("ROW<word STRING>"))
class SplitFunction extends TableFunction[Row]{
  def eval(str: String): Unit = {

    try {
      val list: List[String] = KeywordUtil.splitKeyword(str)
      list.foreach(word => collect(Row.of(word)))
    } catch {
      case e:IOException => collect(Row.of(str))
    }

  }
}
