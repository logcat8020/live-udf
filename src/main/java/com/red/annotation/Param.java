package com.red.annotation;

public @interface Param {
    // 参数类型
    DataType paramType() default @DataType(base = DataTypeEnum.NONE);
    // 参数说明
    String paramDesc() default "";
    // 可选状态 0不可选 1可选
    ParamOptionStatus paramOptionStatus() default ParamOptionStatus.YES;
}
