package com.zhanglei.gmalldatainterface.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Statistic {
    // 指标类型
    String type;
    // 度量值
    Integer orderCt;
}
