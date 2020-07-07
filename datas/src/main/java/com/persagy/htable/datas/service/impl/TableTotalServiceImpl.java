package com.persagy.htable.datas.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.bean.TableInfo;
import com.persagy.htable.datas.service.TableTotalService;
import com.persagy.htable.datas.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TableTotalServiceImpl implements TableTotalService {

    @Override
    public JSONObject getAllTableInfo(String startDate, String endDate, String optionType, String queryTableName) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate);

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);
        ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

        List<TableInfo> tableInfoList = new ArrayList<>();

        Map<String, TableInfo> tableInfoMap = new HashMap<>();

        for (Result result : resultScanner) {

            List<Cell> cells = result.listCells();

            TableInfo tableInfo = HbaseUtils.getTableInfoCell(cells, queryTableName);

            TableInfo info = tableInfoMap.get(tableInfo.getPointTime());

            if (info == null) {
                tableInfoMap.put(tableInfo.getPointTime(), tableInfo);
            } else {
                TableInfo combineTableInfo = this.combineTableInfo(info, tableInfo);
                tableInfoMap.put(tableInfo.getPointTime(), combineTableInfo);
            }

        }

        String jsonString = JSON.toJSONString(tableInfoMap);

        HbaseUtils.closeHbaseConnection(connection);

        return JSON.parseObject(jsonString);
    }

    private TableInfo combineTableInfo(TableInfo t1, TableInfo t2) {

        TableInfo t = new TableInfo();

        t.setTableId(t1.tableId);
        t.setTableName(t1.getTableName());
        t.setPointTime(t1.getPointTime());
        t.setDbName(t1.getDbName());
//        t.setOperationTime(System.currentTimeMillis());

        t.setReadBytes(t1.getReadBytes() + t2.getReadBytes());
        t.setReadLines(t1.getReadLines() + t2.getReadLines());

        t.setInsertBytes(t1.getInsertBytes() + t2.getInsertBytes());
        t.setInsertLines(t1.getInsertLines() + t2.getInsertLines());

        t.setUpdateBytes(t1.getUpdateBytes() + t2.getUpdateBytes());
        t.setInsertLines(t1.getUpdateLines() + t2.getUpdateLines());

        t.setDeleteBytes(t1.getDeleteBytes() + t2.getDeleteBytes());
        t.setDeleteLines(t1.getDeleteLines() + t2.getDeleteLines());

        return t;
    }

}
