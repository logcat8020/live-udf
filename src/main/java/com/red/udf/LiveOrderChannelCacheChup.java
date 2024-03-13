package com.red.udf;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.GlobalPruneTimer;
import cn.hutool.cache.impl.TimedCache;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class LiveOrderChannelCacheChup extends ScalarFunction {

    TimedCache<String, String[]> timedCache = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        //创建缓存，默认3秒过期
        timedCache = CacheUtil.newTimedCache(DateUnit.SECOND.getMillis() * 3);
        timedCache.schedulePrune(DateUnit.SECOND.getMillis() * 30); // 每30秒定时清理
//        scheduleTask(2000); // 测试每个slot的缓存情况,上线时注释
    }

    /**
     * 定时任务:测试缓存
     *
     * @param delay 间隔时长，单位毫秒
     */
    public void scheduleTask(long delay) {
        GlobalPruneTimer.INSTANCE.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println(DateUtil.now() + ",缓存条数：" + timedCache.size());
//                System.out.println(DateUtil.now() + ",缓存容量：" + timedCache.capacity());
            }
        }, delay);
    }

    /**
     *
     * @param cacheSecond 缓存时间，必须比wait的时间长一点
     * @param dataType 区分wait数据，触发更新。1 代表 低wait数据，2 代表高wait数据
     * @param key 缓存的key
     * @param arr 字段
     * @return 返回原字段
     */
    public String[] eval(long cacheSecond,int dataType,String key,String ... arr) {
        if (dataType == 1){
            // 如果是无wait数据，放入缓存，数据正常下发，同时添加一个数据版本字段dataType代表是无wait数据
            timedCache.put(key, arr, DateUnit.SECOND.getMillis() * cacheSecond);
//            Object[] result = Arrays.copyOf(arr, arr.length);
//            result[arr.length - 1] = dataType; // 版本字段必须放在最后
//            return result;
            return arr;
        }else {
            // 如果是wait数据，从缓存中取无wait的数据
            String[] data = timedCache.get(key, false);// 取出数据后，不刷新缓存
            // 如果缓存中没有数据，不进行触发更新，dataType = -1，代表错误
            if (data == null || data.length == 0){
                arr[arr.length - 1] = "-1"; // 错误
                return arr;
            }else {
                // 使用缓存的数据进行下发，不更新订单的状态
                data[data.length - 1] = "2";
                return data;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (timedCache != null) {
            timedCache.clear();
        }
    }

}

