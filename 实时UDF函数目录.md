# 实时UDF函数目录

## 1.order_channel_cache

### 1.1.基本说明

| 项目 | 内容 |
| --- | --- |
| 类名 | com.red.udf.LiveOrderChannelCacheChup |
| 函数名 | order_channel_cache |
| 函数描述 | 订单状态复用缓存器，两条订单流A和B，B订单流延迟5min作为驱动流，复用订单流A的数据，以达到更新订单流A的渠道数据 |
| 函数示例 | select order_channel_cache(cacheSecond,dataType,key,filed_1,filed_2,filed_3) |
| 入参个数 | 4 |
| 返回值 | 数组[STRING] |
| 返回值描述 | 返回原始任意多个字段，字符串类型 |
| 函数状态 | 已上线 |

### 1.2.入参说明

| 参数位置 | 参数类型 | 参数描述 | 可选状态 |
| --- | --- | --- | --- |
| 1 | BIGINT | 缓存时间，必须比wait的时间长一点，单位秒 | 必填 |
| 2 | INT | 区分wait数据，触发更新。1 代表 低wait数据，2 代表高wait数据 | 必填 |
| 3 | STRING | 缓存的key,如 concat(package_id,goods_id) | 必填 |
| 4 | 数组[STRING] | 任意多个字段 | 必填 |

