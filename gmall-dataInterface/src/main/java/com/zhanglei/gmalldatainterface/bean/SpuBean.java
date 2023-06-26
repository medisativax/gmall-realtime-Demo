package com.zhanglei.gmalldatainterface.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SpuBean {
    String spuName;
    Long orderCount;
    Long uuCount;
    Double orderAmount;
}
