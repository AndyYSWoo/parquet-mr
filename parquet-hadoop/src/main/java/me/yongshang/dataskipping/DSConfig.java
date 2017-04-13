package me.yongshang.dataskipping;

/**
 * Created by yongshangwu on 2017/3/20.
 */
public class DSConfig {
    public static boolean ON = true;
    public static int m = 15;
    // add specific features to support filter->feature conversion
    public static String[] features = new String[]{
            "p_type=1",
            // size=m
    };
}
