package com.persagy.htable.datas.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.bean.TableInfo;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import com.persagy.htable.datas.service.TableTotalService;
import com.persagy.htable.datas.utils.CommonUtils;
import com.persagy.htable.datas.utils.DateUtils;
import com.persagy.htable.datas.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TableTotalServiceImpl implements TableTotalService {

    @Override
    public JSONObject getCollectDataByAllTable(String startDate, String endDate, String optionType, String queryTableName) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate + "|");

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> monthDiffList = DateUtils.getDateMonthDiff(startDate, endDate);
        List<String> queryTableNames = new ArrayList<>();
        if (monthDiffList != null && monthDiffList.size() > 0) {

            for (String month : monthDiffList) {

                String queryTableName1 = HbaseDBConstant.ZILLION_META_STAT_1 + month;
            }

        }
        ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

        for (Result result : resultScanner) {

            List<Cell> cells = result.listCells();

            if (cells != null && cells.size() > 0){

                for (Cell cell : cells) {

                    String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                    String flowTime = rowKeys.split(",")[0];

                    byte[] cellBytes = CellUtil.cloneValue(cell);
                    Long value = 0L;
                    if (cellBytes.length > 0) {
                        value = Bytes.toLong(cellBytes);
                    }

                    if (jsonObject.get(flowTime) == null){
                        jsonObject.put(flowTime, value);
                    } else {
                        Long r = (Long) jsonObject.get(flowTime) + value;
                        jsonObject.put(flowTime, r);
                    }

                }

            }

        }

        HbaseUtils.closeHbaseConnection();

        return jsonObject;
    }

    @Override
    public JSONObject getCollectDataBySimpleTable(String startDate, String endDate, String optionType, String queryTableName) {

        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        rowKeyFilterMap.put(startDate, endDate + "|");

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

        for (Result result : resultScanner) {

            List<Cell> cells = result.listCells();

            if (cells != null && cells.size() > 0){

                for (Cell cell : cells) {

                    String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                    String tableName = rowKeys.split(",")[1];

                    byte[] cellBytes = CellUtil.cloneValue(cell);
                    Long value = 0L;
                    if (cellBytes.length > 0) {
                        value = Bytes.toLong(cellBytes);
                    }

                    if (jsonObject.get(tableName) == null){
                        jsonObject.put(tableName, value);
                    } else {
                        Long r = (Long) jsonObject.get(tableName) + value;
                        jsonObject.put(tableName, r);
                    }

                }

            }

        }

        HbaseUtils.closeHbaseConnection();

        return jsonObject;
    }
}
