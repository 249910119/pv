package com.persagy.htable.datas.controller;

import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import com.persagy.htable.datas.service.TableTotalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 首页数据统计
 */
@RestController
public class TableTotalController {

    @Autowired
    TableTotalService tableTotalService;

    /**
     * 首页按日期、选择条件统计数据（饼图、柱状图）
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件（默认查询总数据量?）
     * @return
     */
    @GetMapping("table_total_chart")
    public String getAllTableTotalToChart(@RequestParam("start_date") String startDate,
                              @RequestParam("end_date") String endDate,
                              @RequestParam("option_type") String optionType){

        String queryTableName = HbaseDBConstant.DB_NAME + HbaseDBConstant.ZILLION_META_STAT_2 + "202007";

        JSONObject result = tableTotalService.getCollectDataBySimpleTable(startDate, endDate, optionType, queryTableName);

        return result.toJSONString();
    }

    /**
     * 总数据，按分钟聚合
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param optionType 下拉框选择条件
     * @return
     */
    @GetMapping("table_total")
    public String getAllTableTotal(@RequestParam("start_date") String startDate,
                                       @RequestParam("end_date") String endDate,
                                       @RequestParam("option_type") String optionType){

        String queryTableName = HbaseDBConstant.DB_NAME + HbaseDBConstant.ZILLION_META_STAT_2 + "202007";

        JSONObject result = tableTotalService.getCollectAllTable(startDate, endDate, optionType, queryTableName);

        return result.toJSONString();
    }


}
