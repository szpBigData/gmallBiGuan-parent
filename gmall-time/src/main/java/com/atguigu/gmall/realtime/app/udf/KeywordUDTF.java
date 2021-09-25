package com.atguigu.gmall.realtime.app.udf;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-08-30 23:07
 */
@FunctionHint(output = @DataTypeHint("Row<s String>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value){
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword:keywordList){
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }

    }
}
