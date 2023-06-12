package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class UserRegisterBean(
                           // 窗口起始时间
                           var stt: String,
                           // 窗口结束时间
                           var edt: String,
                           // 注册用户数
                           var registerCt: Long,
                           // 时间戳
                           var ts: Long
                           )
