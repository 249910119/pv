package com.persagy.htable.datas.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import com.persagy.htable.datas.service.SimpleTableService;
import com.persagy.htable.datas.utils.DateUtils;
import com.persagy.htable.datas.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SimpleTableServiceImpl implements SimpleTableService {

    @Override
    public JSONObject getSimpleTable(String startDate, String endDate, String optionType, String tableName) {
        JSONObject jsonObject = new JSONObject();

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> rowKeyFilterMap = new HashMap<>();
        String startRowKey = tableName + "," + DateUtils.getDateMonth(startDate) + "00";
        String endRowKey = tableName + "," + DateUtils.getDateMonth(endDate) + "00";
        rowKeyFilterMap.put(startRowKey, endRowKey);
        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(rowKeyFilterMap);

        List<String> monthDiffList = DateUtils.getDateMonthDiff(startDate, endDate);

        if (monthDiffList != null && monthDiffList.size() > 0){

            for (String month : monthDiffList) {

                String queryTableName = HbaseDBConstant.ZILLION_META_STAT_1 + month;

                ResultScanner resultScanner = HbaseUtils.getResultScanner(connection, queryTableName, optionType, rowKeyFilter);

                for (Result result : resultScanner) {

                    List<Cell> cells = result.listCells();

                    if (cells != null && cells.size() > 0){

                        for (Cell cell : cells) {

                            //zillion_data_ce_computelog, 20200706105000, b2d4f0e74e05478cce244d70ef8d20f7
                            String rowKeys = Bytes.toString(CellUtil.cloneRow(cell));
                            String flowTime = rowKeys.split(",")[1];

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
            }
        }



        HbaseUtils.closeHbaseConnection();

        return jsonObject;
    }
}
