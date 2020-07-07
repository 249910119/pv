package com.persagy.htable.datas.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.service.TableTotalService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class TableTotalServiceImpl implements TableTotalService {


    @Test
    public void getHtable() throws IOException {

        JSONArray result = new JSONArray();
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "192.168.100.51");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        String tableName = "db_public:zillion_meta_stat_2_202007";

        HTable hTable = new HTable(conf, tableName);

        Scan scan = new Scan();

        ResultScanner scanner = hTable.getScanner(scan);

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
        hTable.close();

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


    @Override
    public List<String> getHtablesName() {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "192.168.100.51");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return null;
    }

    @Override
    public JSONObject getAllTableInfo(String startDate, String endDate, String optionType) {
        return null;
    }
}
