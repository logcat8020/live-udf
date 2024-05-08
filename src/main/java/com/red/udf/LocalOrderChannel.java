package com.red.udf;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.red.annotation.*;
import com.red.bean.LocalChannelData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;


/**
 * @author chenhuiup
 * @create 2024-04-26 10:48
 */
@UdfMeta(funcName = "local_order_channel",
        funcDesc = "本地订单归因用户行为排序函数",
        funcExample = "select local_order_channel(channel_type, time, parent_order_id,json_1,json_2,json_3)",
        funcStatus = FunctionStatusEnum.ONLINE,
        funcParam = {@Param(paramType =@DataType(base = DataTypeEnum.INT,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "本地订单类型"),
                @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "第三方创建订单时间"),
                @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "父订单id"),
                @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "用户行为日志，json数组字符串"),
                @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "父订单行为日志，json数组字符串"),
                @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE),paramDesc = "订单行为日志，json数组字符串"),
        },
        funcReturn = @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE), paramDesc = "经过排序后最近的一条行为日志，json字符串"))
@Slf4j
public class LocalOrderChannel extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    /**
     *  1.小程序订单(channelType=202):
     *     1) 如果有父订单，优先使用父订单的归因结果（180天），否则使用行为明细之前的归因结果的最近一次行为
     *     2）如果没有父订单，使用行为明细和之前的归因结果的最近一次行为
     *     3）如果是老订单更新(超过180天统一给flag=0)，没有了行为明细，复用之前的归因结果，如果能复用给标志flag = 1，否则给flag = 0。（修改为sql过滤180天前的订单）
     *     4）使用第三方创建订单时间 time > log_time筛选订单，同时 collect_time的dtm不能大于time的dtm，保证行为日志是最近8天的。
     *     5）使用log_time取最新一条，log_time的逻辑是 if(substr(t1.collect_time,1,19) < substr(t1.dvce_time,1,19) or substr(t1.dvce_time,1,19) <= '2000-01-01 00:00:00',substr(t1.collect_time,1,19),substr(t1.dvce_time,1,19))
     *  2.非小程序订单
     *     1) 如果有父订单，优先使用父订单的归因结果（180天），否则使用行为明细之前的归因结果的最近一次行为
     *     2）如果没有父订单，使用行为明细和之前的归因结果的最近一次行为
     *     3）如果是老订单更新(超过180天统一给flag=0)，没有了行为明细，复用之前的归因结果，如果能复用给标志flag = 1，否则给flag = 0。（修改为sql过滤180天前的订单）
     *     4）使用第三方创建订单时间 time > log_time筛选订单，同时 collect_time的dtm不能大于time的dtm，保证行为日志是最近8天的。
     *     5）使用collect_time取最新一条
     * @param channelType 订单类型
     * @param orderTime 第三方创建订单时间
     * @param parentPackageId 父订单
     * @param jsonRows redtable的json数组
     * @return
     */
    public String eval(Integer channelType,String orderTime,String parentPackageId,String... jsonRows) {
        JSONObject result = new JSONObject();
        try {
            if (channelType == 202){
                if (!StringUtils.isEmpty(parentPackageId) && jsonRows[1].length() > 20){
                    // 预售订单取定金,且可以取得定金的订单
                    result = presale(orderTime,jsonRows[1]);
                }else {
                    // 小程序订单
                    result = orderSort(orderTime,jsonRows[0],jsonRows[2],2);
                }
            }else {
                if (!StringUtils.isEmpty(parentPackageId) && jsonRows[1].length() > 20){
                    // 预售订单取定金,且可以取得定金的订单
                    result = presale(orderTime,jsonRows[1]);
                }else {
                    // 非小程序订单
                    result = orderSort(orderTime,jsonRows[0],jsonRows[2],3);
                }
            }
        } catch (Exception e) {
            log.error("LocalOrderChannel发生异常:{}",e.toString());
        }
        return result.toJSONString();
    }

    private JSONObject orderSort(String orderTime, String actionDetail, String orderAttr,int type) {
        List<LocalChannelData> list = new ArrayList<>();
        String timeFilter = type == 2 ? "log_time":"collect_time";
        if (actionDetail.length() > 20){
            JSONArray jsonArray = JSONObject.parseArray(actionDetail);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                if (filterData(orderTime,jsonObject,type)){
                    list.add(LocalChannelData.builder()
                            .seq(jsonObject.getInteger("seq"))
                            .logTime(jsonObject.getString(timeFilter))
                            .jsonObject(jsonObject)
                            .build());
                }
            }
        }
        if (orderAttr.length() > 20){
            JSONArray jsonArray = JSONObject.parseArray(orderAttr);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                if (filterData(orderTime,jsonObject,type)){
                    list.add(LocalChannelData.builder()
                            .seq(jsonObject.getInteger("seq"))
                            .logTime(jsonObject.getString(timeFilter))
                            .jsonObject(jsonObject)
                            .build());
                }
            }
        }
        if (list.size() > 1){
            list.sort((o1,o2) -> {
                int seqComp = Integer.compare(o1.seq, o2.seq);
                if (seqComp != 0) {
                    return seqComp;
                }
                return o2.logTime.compareTo(o1.logTime);
            });
            return list.get(0).jsonObject;
        } else if (list.size() == 1) {
            return list.get(0).jsonObject;
        }
        return new JSONObject();
    }

    public JSONObject presale(String orderTime,String jsonArrayString){

        JSONArray jsonArray = JSONObject.parseArray(jsonArrayString);
        List<LocalChannelData> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            if (filterData(orderTime,jsonObject,1)){
                list.add(LocalChannelData.builder()
                        .dtm(Integer.parseInt(jsonObject.getString("dtm_dtm")))
                        .jsonObject(jsonObject)
                        .build());
            }
        }
        if (list.size() > 1){
            list.sort((o1, o2) -> o2.dtm - o1.dtm);
            return list.get(0).jsonObject;
        } else if (list.size() == 1) {
            return list.get(0).jsonObject;
        }
        return new JSONObject();
    }

    public boolean filterData(String orderTime,JSONObject jsonObject,int type){
        try {
            int createDtm = Integer.parseInt(DateUtil.format(DateUtil.parse(orderTime), "yyyyMMdd"));
            if (type == 1){
                // 预售取归因后订单，过去180天
                int attrDtm = Integer.parseInt(jsonObject.getString("dtm_dtm"));
                return attrDtm <= createDtm && dateDiff(createDtm,attrDtm) <= 180;
            }else if (type == 2 || type == 3){
                // 订单行为，过去8天的数据，且orderTime > log_time
               int collectDtm = Integer.parseInt(DateUtil.format(DateUtil.parse(jsonObject.getString("collect_time")), "yyyyMMdd"));
               return dateDiff(createDtm,collectDtm) <= 7  && createDtm - collectDtm >= 0 && orderTime.compareTo(jsonObject.getString("log_time")) >= 0;
            }
        } catch (Exception e) {
            log.error("LocalOrderChannel.filterData方法发生异常:{}，数据是:{}",e.getCause(),jsonObject.toJSONString());
        }

        return false;
    }

    public long dateDiff(int date1,int date2){
        return DateUtil.between(DateUtil.parse(String.valueOf(date2)), DateUtil.parse(String.valueOf(date1)), DateUnit.DAY, false);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }

//    public static void main(String[] args) {
//        LocalOrderChannel localOrderChannel = new LocalOrderChannel();
//        System.out.println(localOrderChannel.dateDiff(20240505, 20240428));
//        System.out.println(localOrderChannel.dateDiff(20240505, 20240504));
//        System.out.println(localOrderChannel.dateDiff(20240505, 20240505));
//        System.out.println(localOrderChannel.dateDiff(20240428, 20240505));
//        System.out.println(localOrderChannel.dateDiff(20240505, 20240427));
//        log.error("红红火火恍恍惚惚");
//        System.out.println(Integer.parseInt(DateUtil.format(DateUtil.parse("20240404"), "yyyyMMdd")));
//        ArrayList<LocalChannelData> list = new ArrayList<>();
//        list.add(new LocalChannelData(1,20240416,"2024-04-18 11:11:00",null));
//        list.add(new LocalChannelData(2,20240417,"2024-04-16 11:11:00",null));
//        list.add(new LocalChannelData(3,20240412,"2024-04-18 11:12:00",null));
//        list.add(new LocalChannelData(1,20240417,"2024-04-18 11:11:10",null));
//        list.sort((o1, o2) -> o2.dtm - o1.dtm);
//        System.out.println(list.get(0));
//        list.sort((o1,o2) -> {
//            int seqComp = Integer.compare(o1.seq, o2.seq);
//            if (seqComp != 0) {
//                return seqComp;
//            }
//            return o2.logTime.compareTo(o1.logTime);
//        });
//        System.out.println("============"+ list.get(0));
//        String dateStr1 = "2017-03-01 22:33:23";
//        Date date1 = DateUtil.parse(dateStr1);
//
//        String dateStr2 = "2017-03-01 22:33:23";
//        Date date2 = DateUtil.parse(dateStr2);
//
//        //相差一个月，31天
//        System.out.println(DateUtil.between(date1, date2, DateUnit.SECOND, false));
//        System.out.println("=======:" + dateStr1.compareTo(dateStr2));
//        System.out.println("=============================");
//        LocalOrderChannel localOrderChannel = new LocalOrderChannel();
//        String json1 = "[\n" +
//                "    {\n" +
//                "        \"seq\": 1,\n" +
//                "        \"log_time\": \"2024-04-25 16:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 19:11:00\"\n" +
//                "    },\n" +
//                "    {\n" +
//                "        \"seq\": 2,\n" +
//                "        \"log_time\": \"2024-04-25 11:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 11:12:00\"\n" +
//                "    }\n" +
//                "]";
//
//        String json2 = "[\n" +
//                "    {\n" +
//                "        \"seq\": 1,\n" +
//                "        \"log_time\": \"2024-04-25 11:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 11:11:00\",\n" +
//                "        \"dtm_dtm\": 20240424\n" +
//                "    },\n" +
//                "    {\n" +
//                "        \"seq\": 2,\n" +
//                "        \"log_time\": \"2024-04-25 11:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 11:12:00\"\n" +
//                "    }\n" +
//                "]";
//        String json3 = "[\n" +
//                "    {\n" +
//                "        \"seq\": 1,\n" +
//                "        \"log_time\": \"2024-04-25 15:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 18:11:00\"\n" +
//                "    },\n" +
//                "    {\n" +
//                "        \"seq\": 2,\n" +
//                "        \"log_time\": \"2024-04-25 18:11:00\",\n" +
//                "        \"collect_time\": \"2024-04-25 18:12:00\"\n" +
//                "    }\n" +
//                "]";
//        System.out.println(localOrderChannel.eval(201, "2024-04-25 19:00:00", "", json1, json2, json3));
//
//    }
}
