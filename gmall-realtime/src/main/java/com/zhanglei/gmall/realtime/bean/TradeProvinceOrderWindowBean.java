package com.zhanglei.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * @author zhang_lei
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeProvinceOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 累计下单次数
    Long orderCount;

    // 订单 ID 集合，用于统计下单次数
    @TransientSink
    Set<String> orderIdSet;

    // 累计下单金额
    Double orderAmount;

    // 时间戳
    Long ts;
}
