package com.red.annotation;

import java.lang.annotation.*;

/**
 * udf函数注解，新建udf需要引用该注解
 */
@Documented //文档
@Retention(RetentionPolicy.RUNTIME) //在运行时可以获取
@Target({ ElementType.TYPE, ElementType.METHOD}) //作用到类，方法，接口上等
@Inherited //子类会继承
public @interface UdfMeta {
    // 函数命名
    String funcName();
    // 函数说明
    String funcDesc();
    // 示例
    String funcExample();

    // 函数状态
    FunctionStatusEnum funcStatus() default FunctionStatusEnum.ONLINE;
    // 参数类型，按照参数顺序排列
    Param[] funcParam() default {};//参数可能是多个类型，比如第一个参数可能是int也可能是string，不定长参数默认为空

    // 输出参数
    DataType funcReturn();

    // 输出参数说明
    String funcReturnDesc() default "";
}
