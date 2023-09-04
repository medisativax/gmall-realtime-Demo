package com.zhanglei.gmalldatainterface.contorller;

import com.zhanglei.gmalldatainterface.bean.SpuBean;
import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.bean.Trademark;
import com.zhanglei.gmalldatainterface.mapper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhang_lei
 */
//@Controller
@RestController
@RequestMapping("/api/pyecharts")
@ComponentScan(basePackages = {"com.zhanglei.gmalldatainterface.mapper"})
public class pyechartsController {

    @Autowired
    private GMVMapper gmvMapper;

    @CrossOrigin
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        Double gmv = gmvMapper.selectGMV(date);
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + gmv +
                "}";
    }

    @Autowired
    private StatisticsMapper statisticsMapper;

    @CrossOrigin
    @RequestMapping("/statistics")
    public String getstatistics(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<Statistic> statistisList = statisticsMapper.selectStatistics(date);
        if (statistisList == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < statistisList.size(); i++) {
            Statistic tradeStats = statistisList.get(i);
            String type = tradeStats.getType();
            Integer orderCt = tradeStats.getOrderCt();
            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"value\": " + orderCt + "\n" +
                    "}\n");
            if (i < statistisList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }


    @Autowired
    private ChannelMapper channelMapper;

    @CrossOrigin
    @RequestMapping("/channels")
    public String getChannels(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<Statistic> channelList = channelMapper.selectChannel(date);
        if (channelList == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < channelList.size(); i++) {
            Statistic tradeStats = channelList.get(i);
            String type = tradeStats.getType();
            Integer orderCt = tradeStats.getOrderCt();
            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"value\": " + orderCt + "\n" +
                    "}\n");
            if (i < channelList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }

    @Autowired
    private WordCloudMapper wordCloudMapper;

    @CrossOrigin
    @RequestMapping("/wordCloud")
    public String getWord(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<Statistic> wordList = wordCloudMapper.selectWord(date);
        if (wordList == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < wordList.size(); i++) {
            Statistic tradeStats = wordList.get(i);
            String type = tradeStats.getType();
            Integer orderCt = tradeStats.getOrderCt();
            rows.append("{\n" +
                    "\t\"name\": \"" + type + "\",\n" +
                    "\t\"value\": " + orderCt + "\n" +
                    "}\n");
            if (i < wordList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }


    @Autowired
    private SpuMapper spuMapper;

    @CrossOrigin
    @RequestMapping("/Spu")
    public String getSpu(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<SpuBean> spuBeans = spuMapper.selectSpu(date);
        if (spuBeans == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < spuBeans.size(); i++) {
            SpuBean spuBean = spuBeans.get(i);
            String spuName = spuBean.getSpuName();
            Long orderCount = spuBean.getOrderCount();
            Long uuCount = spuBean.getUuCount();
            Double orderAmount = spuBean.getOrderAmount();
            rows.append("{" +
                    "\t\"spuName\": \"" + spuName + "\",\n" +
                    "\t\"orderCount\": \"" + orderCount + "\",\n" +
                    "\t\"uuCount\": \"" + uuCount + "\",\n" +
                    "\t\"orderAmount\": " + orderAmount + "\n" +
                    "}\n");
            if (i < spuBeans.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }


    @Autowired
    private ProvinceCountMapper provinceCountMapper;

    @CrossOrigin
    @RequestMapping("/provincecount")
    public String getProvinceCount(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<Statistic> provinceCount = provinceCountMapper.selectProvinceCount(date);
        if (provinceCount == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < provinceCount.size(); i++) {
            Statistic province = provinceCount.get(i);
            String type = province.getType();
            Integer orderCt = province.getOrderCt();
            rows.append("{\n" +
                    "\t\"value\": " + orderCt + ",\n" +
                    "\t\"name\": \"" + type + "\"\n" +
                    "}\n");

            if (i < provinceCount.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }

    @Autowired
    private TrademarkMapper trademarkMapper;

    @CrossOrigin
    @RequestMapping("/trademark")
    public String getTrademarkMapper(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }
        List<Trademark> trademarks = trademarkMapper.selectTrademark(date);
        if (trademarks == null){
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < trademarks.size(); i++) {
            Trademark trademark = trademarks.get(i);
            String type = trademark.getTrademarkName();
            List<Double> orderCt = new ArrayList<>();
            orderCt.add(Double.valueOf(trademark.getOrderCount()));
            orderCt.add(Double.valueOf(trademark.getUuCount()));
            orderCt.add(trademark.getOrderAmount());
            orderCt.add(Double.valueOf(trademark.getRefundCount()));
            orderCt.add(Double.valueOf(trademark.getUuRefundCount()));
            rows.append("{\n" +
                    "\t\"value\": " + orderCt + ",\n" +
                    "\t\"name\": \"" + type + "\"\n" +
                    "}\n");

            if (i < trademarks.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{" +
                "    \"status\":0," +
                "    \"msg\":\"\"," +
                "    \"data\": " + rows +
                "}";
    }


    private int getToday(){
        long ts = System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(simpleDateFormat.format(ts));
    }
}
