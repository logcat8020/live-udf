package com.red.bean;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenhuiup
 * @create 2024-04-26 11:32
 */
@NoArgsConstructor
@Data
@AllArgsConstructor
@Builder
public class LocalChannelData {
    public Integer seq;
    public Integer dtm;
    public String logTime;
    public JSONObject jsonObject;
}
