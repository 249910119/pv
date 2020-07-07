package com.persagy.htable.datas.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.service.TableTotalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 单表数据统计
 */
@RestController
public class SimpleTableController {

    @Autowired
    TableTotalService tableTotalService;

    /**
     * 通过首页点击查看表详情以及单表详情页按日期、下拉列表获取表的数据情况
     * @param tableName 表名
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件（默认查询总数据量?）
     * @return
     */
    @GetMapping("simple_table_total")
    public JSONObject getSimpleTableInfo(@RequestParam("table_name") String tableName,
                                         @RequestParam("start_date") String startDate,
                                         @RequestParam("end_date") String endDate,
                                         @RequestParam("option_type") String optionType){
        JSONObject result = new JSONObject();


        return result;
    }
}
