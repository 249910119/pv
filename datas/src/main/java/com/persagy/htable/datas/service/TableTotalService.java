package com.persagy.htable.datas.service;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

public interface TableTotalService {

    /**
     * 获取所有表
     * @param startDate
     * @param endDate
     * @param optionType
     * @param queryTableName
     * @return
     */
    public JSONObject getAllTableInfo(String startDate, String endDate, String optionType, String queryTableName);
}
