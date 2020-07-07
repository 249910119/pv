package com.persagy.htable.datas.test;

import com.persagy.htable.datas.utils.HbaseUtils;
import com.persagy.htable.datas.utils.OptionTypeEnum;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestData {

    @Test
    public void testHbaseConnection() throws IOException {
        Connection connection = HbaseUtils.getConnection();

        Table table = connection.getTable(TableName.valueOf("db_public:zillion_meta_stat_2_202007"));

        Scan scan = new Scan();

        scan.addFamily(Bytes.toBytes("f"));

        String names = OptionTypeEnum.getName("3");
        String[] columnNames = names.split(",");
        for (String columnName : columnNames) {
            scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes(columnName));
        }
//        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("insert_bytes"));

        ResultScanner results = table.getScanner(scan);

        int count = 0;
        for (Result result : results) {

            List<Cell> cells = result.listCells();
            Cell cell1 = cells.get(0);
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
            if (count == 20) {
                break;
            }
        }

        connection.close();
    }


    @Test
    public void testInstance() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Map<String, Student> studentMap = new HashMap<>();

        Student student = new Student();
        student.setName("wdl");

        studentMap.put("a", student);

        System.out.println(studentMap);

        student.setName(student.getName() + "lxm");
        System.out.println(studentMap);

        studentMap.get("a").setName(student.getName() + "lxm");
        System.out.println(studentMap);
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

class Student{

    String name;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                '}';
    }
}