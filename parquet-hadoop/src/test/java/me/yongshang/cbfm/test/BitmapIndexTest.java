package me.yongshang.cbfm.test;

import io.netty.buffer.ByteBuf;
import me.yongshang.cbfm.BitmapIndex;
import me.yongshang.cbfm.CBFM;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import static org.junit.Assert.*;

/**
 * Created by yongshangwu on 2016/11/7.
 */
public class BitmapIndexTest {
    public static void main(String[] args) throws IOException {
        RoaringBitmap r = new RoaringBitmap();
        DecimalFormat F = new DecimalFormat("0.000");
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        for(int i = 0; i < 100000; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            r.add(Integer.valueOf(tokens[2]));
        }
        System.out.println(r.getSizeInBytes());
        r.runOptimize();
        System.out.println(r.getSizeInBytes());
    }
    @Test
    public void testFunctionality(){
        // Table struct:
        // ----A: int
        // ----B: String
        BitmapIndex index = new BitmapIndex(0.1, 1000);
        byte[][] bytes = new byte[][]{
                ByteBuffer.allocate(4).putInt(17).array(),
                "Row1CB".getBytes()
        };
        index.insert(bytes);
        assertTrue(index.contains(bytes));

        bytes = new byte[][]{
                ByteBuffer.allocate(4).putInt(777).array(),
                "Row2CBRatherLong".getBytes()
        };
        index.insert(bytes);
        assertTrue(index.contains(bytes));
        index.displayUsage();
    }
    @Test
    public void testMassively() throws IOException {
        int elementAmount = 100000;
        // Table struct:
        // ----A: int
        // ----B: String
        byte[][][] bytes = new byte[elementAmount][2][];
        BitmapIndex index = new BitmapIndex(0.1, elementAmount);
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        for(int i = 0; i < elementAmount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            index.insert(bytes[i]);
        }
        index.displayUsage();
        for (byte[][] element : bytes) {
            assertTrue(index.contains(element));
        }

//        System.out.println();
//        CBFM.DEBUG = true;
//        CBFM.desired_false_positive_probability_ = 0.1;
//        CBFM.setIndexedDimensions(new String[]{"A", "B"});
//        CBFM.reducedimensions = new int[]{};
//        CBFM cbfm = new CBFM(elementAmount);
    }
}
