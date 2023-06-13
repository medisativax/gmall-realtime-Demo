package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data}

@Data
@AllArgsConstructor
case class TradePaymentWindowBean(
                                   // 窗口起始时间
                                   var stt: String,
                                   // 窗口终止时间
                                   var edt: String,
                                   // 支付成功独立用户数
                                   var paymentSucUniqueUserCount: Long,
                                   // 支付成功新用户数
                                   var paymentSucNewUserCount: Long,
                                   // 时间戳
                                   var ts: Long,
                                 )
