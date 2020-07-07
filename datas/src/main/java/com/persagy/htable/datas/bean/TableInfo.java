package com.persagy.htable.datas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {

    //表id
    public String tableId;

    //表名
    public String tableName;

    //归属数据库
    public String dbName;

    //读行数
    public Long readLines;

    //读字节数
    public Long readBytes;

    //插入行数
    public Long insertLines;

    //插入字节数
    public Long insertBytes;

    //更新行数
    public Long updateLines;

    //更新字节数
    public Long updateBytes;

    //删除行数
    public Long deleteLines;

    //删除字节数
    public Long deleteBytes;

    //操作时间
    public Long operationTime;

    //时间标记
    public String pointTime;

}
