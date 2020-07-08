package com.persagy.htable.datas.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.bean.TableInfo;
import com.persagy.htable.datas.service.TableTotalService;
import com.persagy.htable.datas.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TableTotalServiceImpl implements TableTotalService {

    @Override
    public JSONObject getAllTableInfo(String startDate, String endDate, String optionType, String queryTableName) {

//        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

        Map<String, TableInfo> tableInfoMap = new HashMap<>();

        for (Result result : resultScanner) {

            List<Cell> cells = result.listCells();

            TableInfo tableInfo = HbaseUtils.getTableInfoCell(cells, queryTableName);

            TableInfo info = tableInfoMap.get(tableInfo.getPointTime());

            if (info == null) {
                tableInfoMap.put(tableInfo.getPointTime(), tableInfo);
            } else {
                TableInfo combineTableInfo = this.combineTableInfo(info, tableInfo);
                tableInfoMap.put(combineTableInfo.getPointTime(), combineTableInfo);
            }

        }

        String jsonString = JSON.toJSONString(tableInfoMap);

        HbaseUtils.closeHbaseConnection();

        return JSON.parseObject(jsonString);
    }

//    public TableInfo t = new TableInfo();

    private TableInfo combineTableInfo(TableInfo t1, TableInfo t2) {

//        t1.setTableId(t1.tableId);
//        t1.setTableName(t1.getTableName());
//        t1.setPointTime(t1.getPointTime());
//        t1.setDbName(t1.getDbName());
//        t.setOperationTime(System.currentTimeMillis());

        t1.setReadBytes(t1.getReadBytes() + t2.getReadBytes());
        t1.setReadLines(t1.getReadLines() + t2.getReadLines());

        t1.setInsertBytes(t1.getInsertBytes() + t2.getInsertBytes());
        t1.setInsertLines(t1.getInsertLines() + t2.getInsertLines());

        t1.setUpdateBytes(t1.getUpdateBytes() + t2.getUpdateBytes());
        t1.setInsertLines(t1.getUpdateLines() + t2.getUpdateLines());

        t1.setDeleteBytes(t1.getDeleteBytes() + t2.getDeleteBytes());
        t1.setDeleteLines(t1.getDeleteLines() + t2.getDeleteLines());

        return t1;
    }

}
