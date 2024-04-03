package com.red.annotation;

/**
 * 函数发布状态
 **/
public enum FunctionStatusEnum {
    TEST("测试"),ONLINE("已上线"),ABANDONED("废弃");
    private String chinese;


    FunctionStatusEnum(String chinese) {
        this.chinese = chinese;
    }

    public String getChinese() {
        return chinese;
    }
}
