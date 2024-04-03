package com.red.annotation;

import java.lang.annotation.*;

/**
 * 数据类型注解
 */

@Documented //文档
@Retention(RetentionPolicy.RUNTIME) //在运行时可以获取
@Target({ ElementType.TYPE, ElementType.METHOD}) //作用到类，方法，接口上等
@Inherited //子类会继承
public @interface DataType {
    // hive基本数据类型
    DataTypeEnum base() default DataTypeEnum.NONE;
    // 复合类型
    DataTypeEnum array() default DataTypeEnum.NONE;
    DataTypeEnum[] map() default {};
    // stuct类型
    DataTypeEnum[] struct() default {};

    // 字段类型描述
    DataTypeDescEnum dataTypeDesc() default DataTypeDescEnum.BASE;
}
