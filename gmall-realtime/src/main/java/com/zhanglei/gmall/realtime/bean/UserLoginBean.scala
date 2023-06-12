package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class UserLoginBean (
                         // 窗口起始时间
                         var stt: String,
                         // 窗口结束时间
                         var edt: String,
                         // 回流用户数
                         var backCt: Long,
                         // 独立用户数
                         var uuCt: Long,
                         // 时间戳
                         var ts: Long
                         )
