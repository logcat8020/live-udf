package com.red.annotation;

/**
 *  hive数据类型
 **/
public enum DataTypeDescEnum {

    BASE("基本类型"),ARRAY("数组"),MAP("字典"),STRUCT("结构体");

    private String chinese ;

    DataTypeDescEnum(String chinese) {
        this.chinese = chinese;
    }

    public String getChinese() {
        return chinese;
    }
}


