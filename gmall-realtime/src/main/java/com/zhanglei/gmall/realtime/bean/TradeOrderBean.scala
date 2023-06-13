package com.zhanglei.gmall.realtime.bean

case class TradeOrderBean(
                         // 窗口起始时间
                         var stt: String,
                         // 窗口关闭时间
                         var edt: String,
                         // 下单独立用户数
                         var orderUniqueUserCount: Long,
                         // 下单新用户数
                         var orderNewUserCount: Long,
                         // 下单活动减免金额
                         var orderActivityReduceAmount: Double,
                         // 下单优惠券减免金额
                         var orderCouponReduceAmount: Double,
                         // 下单原始金额
                         var orderOriginalTotalAmount: Double,
                         // 时间戳
                         var ts: Long,
                         )
