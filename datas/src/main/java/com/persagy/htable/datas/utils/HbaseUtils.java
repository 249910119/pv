package com.persagy.htable.datas.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import com.persagy.htable.datas.bean.TableInfo;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseUtils {

    public static Connection connection;

    public static Connection getConnection(){
        Connection connection = null;
        Configuration conf = HBaseConfiguration.create();
        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_QUORUM, HbaseDBConstant.HBASE_ZOOKEEPER_IP);
        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, HbaseDBConstant.HBASE_ZOOKEEPER_PORT);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 初始化 Hbase 连接
     */
    static{

        Configuration conf = HBaseConfiguration.create();

        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_QUORUM, HbaseDBConstant.HBASE_ZOOKEEPER_IP);
        conf.set(HbaseDBConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, HbaseDBConstant.HBASE_ZOOKEEPER_PORT);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * rowKey 过滤
     * @param rowKeyFilterMap key:startKey ; value:endKey
     * @return
     */
    public static Filter getRowKeyFilter(Map<String, String> rowKeyFilterMap){

        Filter rowKeyFilter = null;
        List<MultiRowRangeFilter.RowRange> rangeList_new = new ArrayList<MultiRowRangeFilter.RowRange>();

        try {

            for (Map.Entry<String, String> filterKeys : rowKeyFilterMap.entrySet()) {

                byte[] start = Bytes.toBytes(filterKeys.getKey());
                byte[] end = Bytes.toBytes(filterKeys.getValue());

                MultiRowRangeFilter.RowRange rowRange = new MultiRowRangeFilter.RowRange(start, true, end, true);

                rangeList_new.add(rowRange);
            }

            rowKeyFilter = new MultiRowRangeFilter(rangeList_new);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return rowKeyFilter;
    }

    /**
     * 查询结果
     * @param tableName 查询的表
     * @param optionType 下拉框选择查询的数据
     * @param rowKeyFilter rowKey 过滤器
     * @return
     */
    public static ResultScanner getResultScanner(String tableName,
                                                 String optionType,
                                                 Filter rowKeyFilter
                                                 ) {
        ResultScanner scanner = null;

        try {

            Table htable = connection.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();

            //添加rowKey过滤
            scan.setFilter(rowKeyFilter);

            //添加列族过滤或者列过滤
            String optionTypeNames = OptionTypeEnum.getName(optionType);
            if (optionTypeNames == null || optionTypeNames.isEmpty()) {
                return null;
            }

            for (String columnName : optionTypeNames.split(",")) {
                scan.addColumn(Bytes.toBytes(HbaseDBConstant.FAMILY_NAME),Bytes.toBytes(columnName));
            }

            scanner = htable.getScanner(scan);

            scanner.close();
            htable.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        return scanner;
    }

    /**
     * 关闭连接
     * @param connection
     */
    public static void closeHbaseConnection(Connection connection){
        try {
            if (connection != null){
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private TableInfo getTableInfoCell(List<Cell> cells, String tableName){

        TableInfo tableInfo = new TableInfo();

        if (cells != null && cells.size() > 0){
            Cell cell = cells.get(0);

            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            String[] rowKeyInfos = rowKey.split(",");

            tableInfo.setTableId(rowKey);
            if (tableName.split("_")[4] == "1"){
                tableInfo.setTableName(rowKeyInfos[0]);
                tableInfo.setPointTime(rowKeyInfos[1].substring(0, 12));
            } else {
                tableInfo.setPointTime(rowKeyInfos[0]);
                tableInfo.setTableName(rowKeyInfos[1].substring(0, 12));
            }

            for (Cell c : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(c));
                byte[] cellBytes = CellUtil.cloneValue(c);
                Long value = cellBytes.length > 0 ? Bytes.toLong(cellBytes) : 0;

            }
        }
        return null;
    }

    public JSONArray scanDemo(String table) throws Exception {

        Table htable = connection.getTable(TableName.valueOf(table));

        JSONArray result = new JSONArray();
        Scan scan = new Scan();
        //添加列族过滤或者列过滤
        scan.addFamily(Bytes.toBytes("f"));
        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("read_bytes"));
        //添加rowkey过滤
        byte[] start = Bytes.toBytes("20200706");
        byte[] end = Bytes.toBytes("20200707");

        List<MultiRowRangeFilter.RowRange> rangeList_new = new ArrayList<MultiRowRangeFilter.RowRange>();
        MultiRowRangeFilter.RowRange rowRange = new MultiRowRangeFilter.RowRange(start, true, end, false);
        rangeList_new.add(rowRange);

        Filter rowFilter = new MultiRowRangeFilter(rangeList_new);
        scan.setFilter(rowFilter);

        ResultScanner scanner = htable.getScanner(scan);

        int i = 0;
        while (true) {
            Result res = scanner.next();
            i++;
            if (res == null) {
                break;
            }
            //解析数据
            JSONObject cellResult = getZillionCell(res.listCells());
            System.out.println(i + "," + cellResult);
            result.add(res);
        }
        scanner.close();
        htable.close();
        return result;
    }

    private JSONObject getZillionCell(List<Cell> cells) {
        JSONObject result = new JSONObject();
        for (Cell cell : cells) {
            byte[] row_name_bytes = CellUtil.cloneRow(cell);
            String row_name = Bytes.toString(row_name_bytes);
            byte[] column_name_bytes = CellUtil.cloneQualifier(cell);
            String column_name = Bytes.toString(column_name_bytes);
            byte[] cell_bytes = CellUtil.cloneValue(cell);
            Long value = null;
            if (cell_bytes.length > 0) {
                value = Bytes.toLong(cell_bytes);
            }
            result.put("row", row_name);
            result.put(column_name, value);
        }
        return result;
    }


}
