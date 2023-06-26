package com.zhanglei.gmalldatainterface.mapper;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface WordCloudMapper {

    @Select("select keyword,sum(keyword_count) keyword_count from dws_traffic_source_keyword_page_view_window where toYYYYMMDD(stt) =#{date} group by keyword;")
    List<Statistic> selectWord(@Param("date")int date);
}
