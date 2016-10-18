package org.apache.parquet.hadoop;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Arrays;

/**
 * Created by Yongshang Wu on 2016/10/18.
 */
public class CBFMTest {
    public static void main(String[] args) {
        MessageType SCHEMA = MessageTypeParser.parseMessageType("" +
                "message m {" +
                "    required binary b;" +
                "    required int64 d;" +
                "  }" +
                "}");
        String[] PATH1 = {"b"};
        ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
        String[] PATH2 = {"d"};
        ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);
        System.out.println(Arrays.toString(C1.getPath()));
        System.out.println(Arrays.toString(C2.getPath()));
    }
}
