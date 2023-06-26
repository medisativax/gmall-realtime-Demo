package com.zhanglei.gmalldatainterface.mapper;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface StatisticsMapper {

    @Select("select '下单数' type,\n" +
            "       sum(order_count)        value\n" +
            "from dws_trade_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '下单人数' type,\n" +
            "       count(distinct user_id) value\n" +
            "from dws_trade_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '退单数' type,\n" +
            "       sum(refund_count)       value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '退单人数' type,\n" +
            "       count(distinct user_id) value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            "where toYYYYMMDD(stt) = #{date};")
    List<Statistic> selectStatistics(@Param("date")int date);
}