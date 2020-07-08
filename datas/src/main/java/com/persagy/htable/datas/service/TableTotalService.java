package com.persagy.htable.datas.service;

import com.alibaba.fastjson.JSONObject;

public interface TableTotalService {

    /**
     * 起止日期内,符合 optionType 数据汇总
     * @param startDate
     * @param endDate
     * @param optionType
     * @param queryTableName
     * @return
     */
    public JSONObject getCollectAllTable(String startDate, String endDate, String optionType, String queryTableName);

    /**
     * 按表汇总数据
     * @param startDate
     * @param endDate
     * @param optionType
     * @param queryTableName
     * @return
     */
    public JSONObject getCollectDataBySimpleTable(String startDate, String endDate, String optionType, String queryTableName);
}
