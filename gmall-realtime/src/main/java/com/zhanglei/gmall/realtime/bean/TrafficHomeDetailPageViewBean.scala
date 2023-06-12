package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class TrafficHomeDetailPageViewBean(
                                         // 窗口起始时间
                                         var stt: String,
                                         // 窗口结束时间
                                         var edt: String,
                                         // 首页独立访客数
                                         var homeUvCt: Long,
                                         // 商品详细页独立访客数
                                         var goodDetailUvct: Long,
                                         // 时间戳
                                         var ts: Long
                                         )
