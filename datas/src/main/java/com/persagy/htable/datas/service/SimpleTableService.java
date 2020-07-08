package com.persagy.htable.datas.service;

import com.alibaba.fastjson.JSONObject;

public interface SimpleTableService {

    public JSONObject getSimpleTable(String startDate, String endDate, String optionType, String tableName);

}
