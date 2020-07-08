package com.persagy.htable.datas.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.utils.CommonUtils;
import com.persagy.htable.datas.utils.DateUtils;
import com.persagy.htable.datas.utils.HbaseUtils;
import com.persagy.htable.datas.utils.OptionTypeEnum;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestData {

    @Test
    public void testSubString(){
        String s = "{\"20200706105000\":792753,\"20200706104000\":823481,\"20200706103500\":714471,\"20200706104500\":783199}";
        JSONObject jsonObject = JSONObject.parseObject(s);

    }

    @Test
    public void testJSON(){

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("t1", 109);

        Integer i1 = (Integer) jsonObject.get("221");

        System.out.println(i1);

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
