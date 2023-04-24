package com.yourcompany.util;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Util {
    static final String digits = "0123456789ABCDEF";

    public static String ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return String.valueOf(result);
    }

    public static long getUniqueNumber() {
        return (long) Math.floor(Math.random() * 9_000_000_000L) + 1_000_000_000L;
    }

    public static boolean isNotNullOrEmpty(@Nullable CharSequence charSeq) {
        return charSeq != null && !charSeq.toString().isEmpty();
    }

    static String integerToHex(int input) {
        if (input <= 0)
            return "0";
        StringBuilder hex = new StringBuilder();
        while (input > 0) {
            int digit = input % 16;
            hex.insert(0, digits.charAt(digit));
            input = input / 16;
        }
        return hex.toString();
    }

    public static void main(String[] args) {
        /*StringBuilder hbaseColumnVal = new StringBuilder();
        hbaseColumnVal.append("a").append("-").append("b").append("-");
        hbaseColumnVal = new StringBuilder(hbaseColumnVal.substring(0, hbaseColumnVal.length() - 1));
        System.out.println(hbaseColumnVal);
        String[] test = {"transactiontype", "a","b", "eventtype", "application"};
        Arrays.sort(test);

        int index = Arrays.binarySearch(test, "a");
        System.out.println(Util.ipToLong("192.168.10.11"));
        StringBuilder sb = new StringBuilder("hello:keh");
        Object[] objects = new Object[] {sb};
        System.out.println(objects);*/
        /*List<String> list = new ArrayList<>();
        list.add("a");
        list.add("bn");
        list.add("b");
        System.out.println(Arrays.toString(list.toArray()));
        System.out.println(String.join(",", list));
        Object[] obj = create(list.toArray());
        Object[] obj1 = create("a","b","c");
        System.out.println("hello");*/
        Map<String, Map<String, String>> rowKeyToColumnToColumnValMap = new HashMap<>();
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        rowKeyToColumnToColumnValMap.put("key", map);

        Object[] colToColVal = rowKeyToColumnToColumnValMap.values().toArray();
        for (Object obj : colToColVal) {
            System.out.println(obj);
        }

    }

    public static Object[] create(Object... values) {
        System.out.println(values);
        return values;
    }
}
