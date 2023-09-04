package com.zhanglei.gmalldatainterface.service;

import com.zhanglei.gmalldatainterface.bean.Trademark;

import java.util.List;

public interface TrademarkService {
    List<Trademark> selectTrademark(int date);
}
