package com.zhanglei.gmalldatainterface.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zhang_lei
 */
@Data
@AllArgsConstructor
public class Trademark {
    String trademarkName;
    Long orderCount;
    Long uuCount;
    Double orderAmount;
    Long refundCount;
    Long uuRefundCount;
}
