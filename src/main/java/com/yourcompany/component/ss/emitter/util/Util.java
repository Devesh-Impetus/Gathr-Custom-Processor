package com.yourcompany.component.ss.emitter.util;

import com.yourcompany.component.ss.emitter.hbase.HBaseAccessLayer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.Nullable;

public class Util {
    static final String digits = "0123456789ABCDEF";
    static Configuration conf = HBaseConfiguration.create();

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

    public static void main(String[] args) throws Exception {
        conf.set("hbase.zookeeper.quorum", "comet1.apache.com,comet2.apache.com,comet3.apache.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "172.50.33.73:16010");
        conf.set("hbase.client.scanner.caching", "100");
        conf.set("zookeeper.znode.parent", "/hbase-secure");
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAccessLayer hBaseAccessLayer = new HBaseAccessLayer();
        hBaseAccessLayer.createTable("fpaaa");
        Table table = connection.getTable(TableName.valueOf("demo"));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            System.out.println(result);
        }
        // InputStream payloadInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream("hbaseconfiguration.properties");

       /* Map<String, String> map = new HashMap<>();
        try (InputStream propInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(Constants.HBASE_CONFIG_FILE_NAME)) {
            Properties props = new Properties();
            props.load(propInStr);
            for (Map.Entry<Object, Object> propEntry : props.entrySet()) {
                map.put((String) propEntry.getKey(), (String) propEntry.getValue());
            }
            System.out.println(map.get(Constants.HBASE_FP_TABLE_AAA_TO_EVENT));
        } catch (Exception e) {

        }*/

    }
}
