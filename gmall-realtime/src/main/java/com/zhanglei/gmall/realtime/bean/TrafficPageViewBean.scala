package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class TrafficPageViewBean(
                              // 窗口起始时间
                              var stt: String,
                              // 窗口结束时间
                              var edt: String,
                              // app版本号
                              var vc: String,
                              // 渠道
                              var ch: String,
                              // 地区
                              var ar: String,
                              // 新老访客状态标记
                              var isNew: String,
                              // 独立访客数
                              var uvCt: Long,
                              // 会话数
                              var svCt: Long,
                              // 页面浏览数
                              var pvCt: Long,
                              // 累计访问时长
                              var durSum: Long,
                              // 跳出会话数
                              var ujCt: Long,
                              // 时间戳
                              var ts: Long,
                              )
