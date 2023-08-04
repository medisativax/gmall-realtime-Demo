package com.zhanglei.gmalldatainterface.mapper;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ProvinceCountMapper {
    @Select("select province_name,sum(order_count) order_count from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date} group by province_name")
    List<Statistic> selectProvinceCount(@Param("date")int date);
}
