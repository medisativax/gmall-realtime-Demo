package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class CartAddUuBean(
                          // 窗口起始时间
                          var stt: String,

                          // 窗口闭合时间
                          var edt: String,

                          // 加购独立用户数
                          var cartAddUuCt: Long,

                          // 时间戳
                          var ts: Long,
                        )
