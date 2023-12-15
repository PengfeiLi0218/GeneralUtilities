package priv.lipengfei.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Utils {
    // 将Object列表转为String列表
    public static <T> List<String> objsToString(List<T> objs){
        List<String> ls = new ArrayList<>();
        if(objs==null)
            objs = new ArrayList<>();
        for (Object o : objs) {
            ls.add(o.toString());
        }
        return ls;
    }

    // 生成随机编码
    public static long get64MostSignificantBitsForVersion1() {
        final long currentTimeMillis = System.currentTimeMillis();
        final long time_low = (currentTimeMillis & 0x0000_0000_FFFF_FFFFL) << 32;
        final long time_mid = ((currentTimeMillis >> 32) & 0xFFFF) << 16;
        final long version = 1 << 12;
        final long time_hi = ((currentTimeMillis >> 48) & 0x0FFF);
        return time_low | time_mid | version | time_hi;
    }

    // 判断列表A是否包含列表B，即列表B是否是列表A的子集
    public static <T> boolean subset(List<T> A, List<T> B){
        A.sort(Comparator.comparing(T::hashCode));
        B.sort(Comparator.comparing(T::hashCode));
        return A.containsAll(B);
    }
}
