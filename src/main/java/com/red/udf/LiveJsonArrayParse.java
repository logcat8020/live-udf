package com.red.udf;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;


public class LiveJsonArrayParse extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public String eval(String json,Integer index) {
        try {
            JSONArray jsonArray = JSONArray.parseArray(json);

            if (jsonArray.size() > 0) {
                return jsonArray.getString(index);
            }
        } catch (Exception e) {
            //
        }
        return "";
    }
}
