package com.zhanglei.gmalldatainterface.service;

import com.zhanglei.gmalldatainterface.bean.SpuBean;
import com.zhanglei.gmalldatainterface.bean.Statistic;

import java.util.List;

public interface SpuService {
    List<SpuBean> selectSpu(int date);
}
