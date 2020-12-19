package tech.mlsql.test.binlogserver;

import com.google.common.primitives.Longs;

import java.util.BitSet;

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class Main2 {
    public static void main(String[] args) {
        BitSet bs = new BitSet();
        bs.set(0);
        bs.set(8);
        bs.set(71);
        long bsLong = byteArrayToLong(bs.toByteArray());
        BitSet bs2 = BitSet.valueOf(Longs.toByteArray(Long.reverseBytes(bsLong)));
        System.out.println(bs2.get(1));
        System.out.println(bs2.get(0));
        System.out.println(bs2.get(8));
        System.out.println(bs2.get(71));
        System.out.println(bs.get(71));
    }


    public static long byteArrayToLong(byte[] by) {
        long longValue = 0;
        for (int i = 0; i < by.length; i++) {
            longValue += ((long) by[i] & 0xffL) << (8 * i);
        }
        return longValue;
    }
}
