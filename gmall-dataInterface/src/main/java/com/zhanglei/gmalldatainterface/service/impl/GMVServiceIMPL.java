package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.mapper.GMVMapper;
import com.zhanglei.gmalldatainterface.service.GMVService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GMVServiceIMPL implements GMVService {
    @Autowired
    private GMVMapper gmvMapper;

    @Override
    public Double getGMV(int date) {
        return gmvMapper.selectGMV(date);
    }
}
