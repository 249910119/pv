package com.persagy.htable.datas.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.bean.TableInfo;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class HbaseUtils {

    public static Connection connection = null;
    public static ResultScanner scanner = null;
    public static Table htable =  null;

    public static Connection getConnection(){

//        Connection connection = null;
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
     * 获取所有 nameSpace 名字
     * @param connection
     * @return
     */
    public static List<String> getAllNameSpace(Connection connection){

        List<String> nameSpaceList = new ArrayList<>();

        try {

            Admin admin = connection.getAdmin();

            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                String name = namespaceDescriptor.getName();
                if ("default".equals(name) || "hbase".equals(name))
                nameSpaceList.add(name);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return nameSpaceList;
    }

    /**
     * 初始化 Hbase 连接
     */
    /*
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
    */

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
     * @param queryTableName 查询的表
     * @param optionType 下拉框选择查询的数据
     * @param rowKeyFilter rowKey 过滤器
     * @return
     */
    public static ResultScanner getResultScanner(Connection connection,
                                                 String queryTableName,
                                                 String optionType,
                                                 Filter rowKeyFilter
                                                 ) {
        try {

            htable = connection.getTable(TableName.valueOf(queryTableName));

            Scan scan = new Scan();

            //添加rowKey过滤
            if (rowKeyFilter != null){
                scan.setFilter(rowKeyFilter);
            }

            //添加列族过滤或者列过滤
            String optionTypeNames = OptionTypeEnum.getName(optionType);
            if (optionTypeNames == null || optionTypeNames.isEmpty()) {
                return null;
            }

            scan.addFamily(Bytes.toBytes(HbaseDBConstant.FAMILY_NAME));

            for (String columnName : optionTypeNames.split(",")) {
                scan.addColumn(Bytes.toBytes(HbaseDBConstant.FAMILY_NAME),Bytes.toBytes(columnName));
            }

            scanner = htable.getScanner(scan);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return scanner;
    }

    /**
     * 关闭连接
     */
    public static void closeHbaseConnection(){
        try {
            if (connection != null){
                connection.close();
            }
            if (scanner != null){
                scanner.close();
            }
            if (htable != null){
                htable.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
