package com.red.utils;

import cn.hutool.core.util.ClassUtil;
import com.red.annotation.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 * @author chenhuiup
 * @create 2024-04-01 17:30
 */
public class GeneratorMarkdown {

    public static void generatorMarkdown(String packageName,String title,String filePath) {
        Set<Class<?>> classes = ClassUtil.scanPackage(packageName);
        MarkdownBuilder builder = new MarkdownBuilder();
        builder.header1(title);
        int count = 0;
        for (Class<?> clazz : classes) {

            if (clazz.isAnnotationPresent(UdfMeta.class)){
                count += 1;
                int inner = 1;
                UdfMeta info = clazz.getAnnotation(UdfMeta.class);
                Param[] params = info.funcParam();
                String[][] data = getParamData(params);

                builder.header2(count + "."+ info.funcName())
                        .header3(count + "." + inner + ".基本说明")
                        .table(new String[]{"项目","内容"},new String[][]{
                                {"类名",clazz.getName()},
                                {"函数名",info.funcName()},
                                {"函数描述", info.funcDesc()},
                                {"函数示例",info.funcExample()},
                                {"入参个数",String.valueOf(info.funcParam().length)},
                                {"返回值类型",getParamType(info.funcReturn().paramType())},
                                {"返回值描述",info.funcReturn().paramDesc()},
                                {"函数状态",info.funcStatus().getChinese()}
                        });
                if (params.length > 0) {
                    builder.header3(count + "." + (inner + 1) + ".入参说明")
                            .table(new String[]{"参数位置","参数类型","参数描述","可选状态"},
                                    data);
                }
            }
        }
        try {
            FileWriter writer = new FileWriter(filePath);
            writer.write(builder.build());
            writer.close();
            System.out.println("~~成功~~");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private static String[][] getParamData(Param[] params) {
        String[][] data = null;
        if (params.length > 0){
            data = new String[params.length][4];
            int i = 0;
            for (Param param : params) {
                i ++;
                DataType dataType = param.paramType();
                String paramType = getParamType(dataType);
                data[i - 1] = new String[]{String.valueOf(i),paramType,param.paramDesc(),param.paramOptionStatus().getChinese()};
            }
        }
        return data;
    }

    private static String getParamType(DataType dataType) {
        DataTypeDescEnum dataTypeDescEnum = dataType.dataTypeDesc();
        String paramType = "";
        if (dataTypeDescEnum == DataTypeDescEnum.BASE){
            paramType = dataType.base().getChinese();
        } else if (dataTypeDescEnum == DataTypeDescEnum.ARRAY){
            paramType = DataTypeDescEnum.ARRAY.getChinese() + "[" + dataType.array().getChinese() + "]";
        } else if (dataTypeDescEnum == DataTypeDescEnum.MAP) {
            ArrayList<String> list = new ArrayList<>();
            for (DataTypeEnum item : dataType.map()) {
                list.add(item.getChinese());
            }
            paramType = DataTypeDescEnum.MAP.getChinese() + list;
        } else {
            ArrayList<String> list = new ArrayList<>();
            for (DataTypeEnum item : dataType.struct()) {
                list.add(item.getChinese());
            }
            paramType = DataTypeDescEnum.STRUCT.getChinese() + list;
        }
        return paramType;
    }

}
