package com.zhanglei.gmall.realtime.bean

case class TableProcess(
  // 来源表
  var sourceTable: String,
  // 输出表
  var SinkTable: String,
  // 输入字段
  var sinkPk: String,
  // 主键字段
  var sinkColumns: String,
  // 建表扩展字段
  var sinkExtend: String

  )
