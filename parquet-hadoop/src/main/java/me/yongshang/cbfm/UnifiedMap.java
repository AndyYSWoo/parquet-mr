package me.yongshang.cbfm;

import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class UnifiedMap {
    private boolean isLast;
    private HashMap<Integer, UnifiedMap> midMap;
    private RoaringBitmap bitmap;

    public UnifiedMap(boolean isLast){
        setLast(isLast);
        if(isLast){
            bitmap = new RoaringBitmap();
        }else{
            midMap = new HashMap<>();
        }
    }

    public boolean isLast() {
        return isLast;
    }

    public void setLast(boolean last) {
        isLast = last;
    }

    public HashMap<Integer, UnifiedMap> getMidMap() {
        return midMap;
    }

    public void setMidMap(HashMap<Integer, UnifiedMap> midMap) {
        this.midMap = midMap;
    }

    public RoaringBitmap getBitmap() {
        return bitmap;
    }

    public void setBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }
}
