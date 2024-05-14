package com.red.udf;

import com.red.annotation.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * 直播流量: traffic_type字段解析
 * 参考文档: https://doc.weixin.qq.com/doc/w3_AfwA9wYgAKYkyALA1NtQIeTscsGDx?scode=ANAAyQcbAAgJZH019uAfwA9wYgAKY
 */
@UdfMeta(funcName = "traffic_type",
        funcDesc = "直播流量traffic_type字段解析，参考文档: https://doc.weixin.qq.com/doc/w3_AfwA9wYgAKYkyALA1NtQIeTscsGDx?scode=ANAAyQcbAAgJZH019uAfwA9wYgAKY",
        funcExample = "select live_traffic_type_chup(first_level_channel)",
        funcStatus = FunctionStatusEnum.ONLINE,
        updateLog = {"20240424,兼顾ads_track_id,[参考链接](https://docs.xiaohongshu.com/doc/dd2dd5d6897b6565efaeb12c8e322446)",
                "20240514,从“主播扶持”实现逻辑中剔除掉ads_track_id的判断;剔除掉carrier_page_pre的判断。[参考链接](https://docs.xiaohongshu.com/doc/2291c8693a40363e07a4f0ee0530042e)",},
        funcParam = {@Param(paramType =@DataType(map ={DataTypeEnum.STRING,DataTypeEnum.STRING},dataTypeDesc = DataTypeDescEnum.MAP),paramDesc = "map类型的渠道参数")},
        funcReturn = @Param(paramType =@DataType(base = DataTypeEnum.STRING,dataTypeDesc = DataTypeDescEnum.BASE), paramDesc = "枚举值：自然流量、rocket推广-硬保、rocket推广-软保、主播扶持"))
public class LiveTrafficTypeChupV2 extends ScalarFunction {


    private HashSet<String> s1 = new HashSet<>();
    private HashSet<String> s2 = new HashSet<>();
    private HashSet<String> s3 = new HashSet<>();
    private HashSet<String> s4 = new HashSet<>();

    @Override
    public void open(FunctionContext context) throws Exception {
        s1.addAll(Arrays.asList("explore_feed", "search_result_notes"));
        s2.addAll(Arrays.asList("live_view_page", "note_detail_r10", "video_feed", "video_channel_feed", "user_page"));
        s3.addAll(Arrays.asList("live_view_page", "note_detail_r10", "video_feed", "video_channel_feed"));
        s4.addAll(Arrays.asList("note_detail_r10", "video_feed", "video_channel_feed"));
    }


    public String eval(Map<String, String> firstLevelChannel) {
        try {

            // 兼容传参为null情形
//            if (carrierPage == null) {
//                carrierPage = new HashMap<>();
//            }
//            if (carrierPagePre == null) {
//                carrierPagePre = new HashMap<>();
//            }
//            if (firstLevelChannel == null) {
//                firstLevelChannel = new HashMap<>();
//            }
//            if (lastChannel == null) {
//                lastChannel = new HashMap<>();
//            }

            // 获取当前页面名称
            String pageInstance = coalesce(firstLevelChannel.getOrDefault("page_instance", ""));
            // 获取当前点位的source信息
            String liveSource = coalesce(firstLevelChannel.getOrDefault("live_target_live_source", ""));
            // 获取当前点位的pre_source信息
            String preSource = coalesce(firstLevelChannel.getOrDefault("pre_source", ""));

            // 获取一级渠道页面名称
            String firstPageInstance = coalesce(firstLevelChannel.getOrDefault("page_instance", ""));
            String firstAction = coalesce(firstLevelChannel.getOrDefault("action", ""));
            String firstPointId = coalesce(firstLevelChannel.getOrDefault("point_id", ""));

            // 获取上一来源的页面名称
            String prePageInstance = coalesce(firstLevelChannel.getOrDefault("page_instance", ""));

            // ads_target_track_id
            String adsTrackId = coalesce(firstLevelChannel.getOrDefault("ads_target_track_id", ""));
            String preAdsTrackId = coalesce(firstLevelChannel.getOrDefault("ads_target_track_id", ""));

            // note_target_tracker_id
            String noteTrackId = coalesce(firstLevelChannel.getOrDefault("note_target_tracker_id", ""));
            String preNoteTrackId = coalesce(firstLevelChannel.getOrDefault("note_target_tracker_id", ""));

            // live_target_track_id
            String liveTrackId = coalesce(firstLevelChannel.getOrDefault("live_target_track_id", ""));
            String preLiveTrackId = coalesce(firstLevelChannel.getOrDefault("live_target_track_id", ""));

            // last_channel
            String lastPageInstance = coalesce(firstLevelChannel.getOrDefault("page_instance", ""));


            // 20231013分区开始生效
            if ("deeplink_jump_page".equals(lastPageInstance) || "share_out_of_app".equals(liveSource) || "share_out_of_app".equals(preSource)) {
                return "自然流量";
            }
            if (like(adsTrackId, "%ads%", "%")
                    || ("live_view_page".equals(pageInstance) && s1.contains(prePageInstance) && like(preAdsTrackId, "%ads%", "%"))
                    || (s2.contains(pageInstance) && s3.contains(prePageInstance) && !pageInstance.equals(prePageInstance) && like(preAdsTrackId, "%ads%", "%"))
                    || Arrays.asList("29750", "1876").contains(firstPointId)
            ) {
                return "广告";
            }
            // 场域traffic_type字段，下游使用时发现：本应属于“rocket推广_硬保”归到了“自然流量”。究其原因，是打点侧打点有误，需要兼容ads_track_id参数。
            // 参考文档：https://docs.xiaohongshu.com/doc/dd2dd5d6897b6565efaeb12c8e322446
            if (
                    (like(adsTrackId, "tselected_%", "%") || (s2.contains(pageInstance) && s4.contains(prePageInstance) && !pageInstance.equals(prePageInstance) && like(preAdsTrackId, "tselected_%", "%")))
                            || (like(noteTrackId, "tselected_%", "%") || (s2.contains(pageInstance) && s4.contains(prePageInstance) && !pageInstance.equals(prePageInstance) && like(preNoteTrackId, "tselected_%", "%")))
            ) {
                return "rocket推广-硬保";
            }
            if (
                    (like(adsTrackId, "boost_%", "%") || (s2.contains(pageInstance) && s4.contains(prePageInstance) && !pageInstance.equals(prePageInstance) && like(preAdsTrackId, "boost_%", "%")))
                            || (like(noteTrackId, "boost_%", "%") || (s2.contains(pageInstance) && s4.contains(prePageInstance) && !pageInstance.equals(prePageInstance) && like(preNoteTrackId, "boost_%", "%")))
            ) {
                return "rocket推广-软保";
            }
            // 参考:https://docs.xiaohongshu.com/doc/2291c8693a40363e07a4f0ee0530042e
            if (     like(liveTrackId, "%support%", "%")
                    || like(liveTrackId, "%launch%@%", "%")
            ) {
                return "主播扶持";
            }
            return "自然流量";
        } catch (Exception e) {
            //
        }
        return "自然流量";
    }

    public String coalesce(String data,String defaultValue){
        return data == null? defaultValue:data;
    }

    public String coalesce(String data){
        return coalesce(data,"");
    }


    // java实现like逻辑
//    public boolean like(String oriText, String template, String matcher) {
//        if (oriText == null && template == null) {
//            return true;
//        }
//        if (oriText == null || template == null) {
//            return false;
//        }
//        oriText = oriText.trim();
//        template = template.trim();
//        String matcherStr = String.valueOf(matcher);
//        if (!template.contains(matcherStr)) {
//            return oriText.equals(template);
//        }
//        String[] split = template.trim().split(matcherStr);
//        if (!template.startsWith(matcherStr) && !oriText.startsWith(split[0])) {
//            return false;
//        }
//        if (!template.endsWith(matcherStr) && !oriText.endsWith(split[split.length - 1])) {
//            return false;
//        }
//        int i = 0;
//        int beginIndex = 0;
//        while (beginIndex < oriText.length() && i < split.length) {
//            String node = split[i];
//            if (node != null) {
//                int indexOfNode = oriText.indexOf(node);
//                if (indexOfNode != -1) {
//                    beginIndex = indexOfNode + node.length();
//                } else {
//                    return false;
//                }
//            }
//            i++;
//        }
//        return i >= split.length;
//    }

    // java实现like逻辑
    public boolean like(String oriText, String template, String matcher){
        template = template.replace(matcher,"*");
        StringBuilder out = new StringBuilder("^");
        for(int i = 0; i < template.length(); ++i) {
            final char c = template.charAt(i);
            switch(c) {
                case '*': out.append(".*"); break;
                case '?': out.append('.'); break;
                case '.': out.append("\\."); break;
                case '\\': out.append("\\\\"); break;
                default: out.append(c);
            }
        }
        out.append('$');
        template = out.toString();
        return oriText.matches(template);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }
}
