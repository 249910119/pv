package com.persagy.htable.datas.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.utils.CommonUtils;
import com.persagy.htable.datas.utils.DateUtils;
import com.persagy.htable.datas.utils.HbaseUtils;
import com.persagy.htable.datas.utils.OptionTypeEnum;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.lf5.util.DateFormatManager;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestData {

    public String subTableName(String rowKey){

        StringBuffer buff = new StringBuffer("");

        if (rowKey != null && !"".equals(rowKey)){

            String s = rowKey.split(",")[1].split("\\.")[0];

            String[] t = s.split("_");

            for (int i = 2; i < t.length; i++) {
                if (i < t.length - 1){
                    buff.append(t[i]).append("_");
                } else {
                    buff.append(t[i]);
                }
            }
        }

        return buff.toString();
    }

    @Test
    public void testDiff(){
        String s = "zillion_data_ce_computelog,20200706105000,b2d4f0e74e05478cce244d70ef8d20f7";
        String s1 = "20200706103500,zillion_index_fjd_0_computedetail.index_1,bd4eea8315889ef97225dabf39e4dd79";
//        String s1 = "20200706104000,zillion_data_fjd_0_metercomputetime,d6d0314a25f5b3cda1dfe5ac4eabdf39";
        String diff = this.subTableName(s1);
        System.out.println(diff);
    }

    @Test
    public void testSubString() throws ParseException {
        String d = "20200706103500";
        SimpleDateFormat format = new SimpleDateFormat("yyyyMM");

        Date date1 = format.parse("202007");
        Date date2 = format.parse("202109");

        List<String> dateMonthDiff = DateUtils.getDateMonthDiff("202002", "202009");
        System.out.println(dateMonthDiff);
    }

    @Test
    public void testJSON() throws IOException {

        Connection connection = HbaseUtils.getConnection();
        Admin admin = connection.getAdmin();

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            String name = namespaceDescriptor.getName();
            if ("default".equals(name) || "hbase".equals(name)) continue;
            System.out.println(name);
        }

        HbaseUtils.closeHbaseConnection();

    }

    @Test
    public void testHbaseConnection() throws IOException {

        Connection connection = HbaseUtils.getConnection();

        Map<String, String> map = new HashMap<>();

        map.put("202007061035", "202007061045");

        Filter rowKeyFilter = HbaseUtils.getRowKeyFilter(map);
        ResultScanner resultScanner = HbaseUtils.getResultScanner(connection,
                "db_public:zillion_meta_stat_2_202007",
                "1",
                rowKeyFilter);


        int count = 0;
        for (Result result : resultScanner) {

            List<Cell> cells = result.listCells();

            for (Cell cell : cells) {
                System.out.println("第" + count + "个cell");

                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));

                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));

                byte[] cellBytes = CellUtil.cloneValue(cell);
                Long value = null;
                if (cellBytes.length > 0) {
                    System.out.println(Bytes.toLong(cellBytes));
                }
            }
            count ++;
            /*if (count == 20) {
                break;
            }*/
        }

        connection.close();
    }


    @Test
    public void testInstance() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Date date = new Date();

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

        String dateToString = format.format(date);

        System.out.println(dateToString);
    }


    @Test
    public void testEnum(){
        String name = OptionTypeEnum.getName("1");
        String[] split = name.split(",");
        for (String s : split) {
            System.out.println(s);
        }
    }
}
