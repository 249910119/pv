package com.persagy.htable.datas.service;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

public interface TableTotalService {

    /**
     * 获取所有表名字
     * @return
     */
    public List<String> getHtablesName();

    public JSONObject getAllTableInfo(String startDate, String endDate, String optionType);
}
