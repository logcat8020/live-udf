package com.red.annotation;

/**
 * @author chenhuiup
 * @create 2024-04-01 19:05
 */
public enum ParamOptionStatus {
    YES("必填"),NO("非必填");
    private String chinese ;

    ParamOptionStatus(String chinese) {
        this.chinese = chinese;
    }

    public String getChinese() {
        return chinese;
    }
}
