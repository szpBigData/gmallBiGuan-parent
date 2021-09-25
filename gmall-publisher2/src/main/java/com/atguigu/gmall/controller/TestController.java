package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.mapper2.ProvinceStatsMapper;
import com.atguigu.gmall.service2.impl.ProvinceStatsServiceImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-09-25 7:51
 */
@RestController
@RequestMapping("/api/sugar2")
public class TestController {
//    {
//        "status": 0,
//            "msg": "",
//            "data": 1201081.1632389291
//    }
@Autowired
    ProvinceStatsServiceImpl provinceStatsService;
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date==0){
            date=now();
        }
        StringBuilder json=new StringBuilder("  {\n" +
                "        \"status\": 0,\n" +
                "            \"msg\": \"\",\n" +
                "            \"data\": 1201081.1632389291\n" +
                "    }");
        return json.toString();
    }
    /*
{
    "status": 0,
    "data": {
        "columns": [
            { "name": "商品名称",  "id": "spu_name"
            },
            { "name": "交易额", "id": "order_amount"
            }
        ],
        "rows": [
            {
                "spu_name": "小米10",
                "order_amount": "863399.00"
            },
           {
                "spu_name": "iPhone11",
                "order_amount": "548399.00"
            }
        ]
    }
}
 */
    @RequestMapping("/spu")
public String getSpu(@RequestParam(value = "date" ,defaultValue = "0") Integer date,
                     @RequestParam(value = "limit",defaultValue = "10") int limit){
    if (date==0){
        date=now();
    }
    //设置表头
        StringBuilder jsonBuilder=new StringBuilder("{\n" +
                "    \"status\": 0,\n" +
                "    \"data\": {\n" +
                "        \"columns\": [\n" +
                "            { \"name\": \"商品名称\",  \"id\": \"spu_name\"\n" +
                "            },\n" +
                "            { \"name\": \"交易额\", \"id\": \"order_amount\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"rows\": [");
    return "";
}
//    {
//        "status": 0,
//            "data": {
//        "mapData": [
//        {
//            "name": "北京",
//                "value": 9131
//        },
//        {
//            "name": "天津",
//                "value": 5740
//        }
//       ]
//    }
//    }

@RequestMapping("/province")
public String getProvinceStats(@RequestParam(value = "date",defaultValue = "0")Integer date){
        if (date==0){
            date=now();
        }
        StringBuilder jsonBuilder=new StringBuilder(" {\n" +
                "        \"status\": 0,\n" +
                "            \"data\": {\n" +
                "        \"mapData\": [\n" +
                "        {");
    List<ProvinceStats> provinceStatsList = provinceStatsService.selectProvinceStats(date);
    if (provinceStatsList.size()==0){

    }
    for (int i=0;i<provinceStatsList.size();i++){
        if (i>=1){
            jsonBuilder.append(",");
        }
        ProvinceStats provinceStats = provinceStatsList.get(i);
        jsonBuilder.append("{\n" +
                "            \"name\": \""+"123"+",\n" +
                "                \"value\":" +"789" +"\n"+
                "        }");
    }
    jsonBuilder.append("   ]\n" +
                "    }\n" +
                "    }");
        return jsonBuilder.toString();

}
public String sun(){
        return "sunzhipeng";
}
    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
