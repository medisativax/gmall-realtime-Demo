package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.Trademark;
import com.zhanglei.gmalldatainterface.mapper.TrademarkMapper;
import com.zhanglei.gmalldatainterface.service.TrademarkService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class TrademarkIMPL implements TrademarkService {
    @Autowired
    private TrademarkMapper trademarkMapper;

    @Override
    public List<Trademark> selectTrademark(int date) {
        return trademarkMapper.selectTrademark(date);
    }
}
