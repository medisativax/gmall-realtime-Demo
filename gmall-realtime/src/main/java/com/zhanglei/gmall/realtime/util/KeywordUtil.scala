package com.zhanglei.gmall.realtime.util

import org.wltea.analyzer.core.{IKSegmenter, Lexeme}

import java.io.{IOException, StringReader}
import java.util
import scala.collection.mutable.ListBuffer

object KeywordUtil {
  def splitKeyword(keyword:String): List[String] ={
    //创建集合用于存放切分后的数据
    val list = new ListBuffer[String]

    //创建IK分词对象
    val reader = new StringReader(keyword)

    val ikSegmenter = new IKSegmenter(reader, false)

    //取出切分完成的词
    try {
      var lexeme: Lexeme = null
      while ( {
        lexeme = ikSegmenter.next(); lexeme != null
      }) {
        val word = lexeme.getLexemeText
        list.append(word)
      }
    } catch {
      case e:IOException => throw new IOException
    }

    //返回集合
    return list.toList
  }

}
