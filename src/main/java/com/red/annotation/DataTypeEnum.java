package com.red.annotation;

/**
 *  hive数据类型
 **/
public enum DataTypeEnum {
    // 空
    NONE("不填写"),
    // 数字类
    TINYINT("TINYINT"),SMALLINT("SMALLINT"),INT("INT"),BIGINT("BIGINT"),FLOAT("FLOAT"),DOUBLE("DOUBLE"),DECIMAL("DECIMAL"),
    // 日期时间类
    TIMESTAMP("TIMESTAMP"),DATE("DATE"),INTERVAL("INTERVAL"),
    // 字符串类
    STRING("STRING"),VARCHAR("VARCHAR"),CHAR("CHAR"),
    // MISC类
    BOOLEAN("BOOLEAN"),BINARY("BINARY");

    private String chinese ;

    DataTypeEnum(String chinese) {
        this.chinese = chinese;
    }

    public String getChinese() {
        return chinese;
    }
}


