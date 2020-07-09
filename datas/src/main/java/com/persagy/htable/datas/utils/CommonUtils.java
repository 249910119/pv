package com.persagy.htable.datas.utils;

import com.alibaba.fastjson.JSONObject;
import com.persagy.htable.datas.constant.HbaseDBConstant;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 工具包
 */
public class CommonUtils {

    /**
     * 截取表名
     * @param name
     * @return
     */
    public static String subTableName(String name){

        StringBuffer buff = new StringBuffer("");

        if (name != null && !"".equals(name)){

            String s = name.split("\\.")[0];

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
}
