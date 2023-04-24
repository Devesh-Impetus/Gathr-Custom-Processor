package com.yourcompany.component.ss.emitter.hbase;

import com.yourcompany.component.ss.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class HBaseAccessLayer {
    static Configuration hconfiguration = null;
    static Connection connection = null;
    static boolean isInit = false;
    static Logger log = LoggerFactory.getLogger(HBaseAccessLayer.class.getName());
    private static Map<String, String> hbaseConfigProp;
    long batchSize;
    private ResultScanner objResultScanner;

    public HBaseAccessLayer() {
        hbaseConfigProp = new HashMap<>();
        try (InputStream propInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(Constants.HBASE_CONFIG_FILE_NAME)) {
            Properties props = new Properties();
            props.load(propInStr);
            for (Map.Entry<Object, Object> propEntry : props.entrySet()) {
                hbaseConfigProp.put((String) propEntry.getKey(), (String) propEntry.getValue());
            }
            batchSize = Long.parseLong(hbaseConfigProp.get(Constants.HBASE_BATCH_SIZE));
            init();
        } catch (Exception e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
        }
    }

    //intilize the hbase layer.
    public static boolean init() throws Exception {
        if (!isInit) {
            hconfiguration = HBaseConfiguration.create();

            for (Map.Entry<String, String> entry : hbaseConfigProp.entrySet()) {
                hconfiguration.set(entry.getKey(), entry.getValue());
            }


            connection = ConnectionFactory.createConnection(hconfiguration);

            isInit = true;
        }
        return isInit;
    }

    public int InsertRecord(String tableName, List<Put> lstrecords, int count) throws Exception {
        log.info("Entered At " + this.getClass().getName() + "InsertRecord();");
        log.info("Entered At " + this.getClass().getName() + " Inserting the " + lstrecords.size() + "record on Table " + tableName);
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Object[] results = new Object[lstrecords.size()];
            table.batch(lstrecords, results);
            count = -1;
            table.close();
        } catch (TableNotFoundException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
            try {
                createTable(tableName);
            } catch (Exception e1) {
                StringWriter errors1 = new StringWriter();
                e.printStackTrace(new PrintWriter(errors1));
                String error1 = e.toString();
                log.error(error1);
                Writer writer = new StringWriter();
                PrintWriter printWriter = new PrintWriter(writer);
                e.printStackTrace(printWriter);
                String s = writer.toString();
                log.error(s);
            }
            Table table = connection.getTable(TableName.valueOf(tableName));
            Object[] results = new Object[lstrecords.size()];
            table.batch(lstrecords, results);
            count--;
            log.debug("Table created " + tableName);
        } catch (TableExistsException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Object[] results = new Object[lstrecords.size()];
            table.batch(lstrecords, results);
            table.close();
            count--;
            log.debug("Table already exists " + tableName);
        } catch (IOException e) {
            count--;
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
            log.error("exception occured while fetching the data " + this.getClass().getName() + " exception details: " + e.getMessage());
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
        }

        log.info("Exited from " + this.getClass().getName() + "InsertRecord();");
        return count;
    }

    public void createTable(String tablename) throws Exception {
        log.info("CreateTable Call");

        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(tablename);
        HTableDescriptor tableDesc = new HTableDescriptor(table);
        HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(Constants.FACEBOOK_ID));
        family.setCompressionType(Algorithm.GZ);
        tableDesc.addFamily(family);
        family = new HColumnDescriptor(Bytes.toBytes(Constants.TWITTER_ID));
        family.setCompressionType(Algorithm.GZ);
        tableDesc.addFamily(family);
        family = new HColumnDescriptor(Bytes.toBytes(Constants.YOUTUBE_ID));
        family.setCompressionType(Algorithm.GZ);
        tableDesc.addFamily(family);
        family = new HColumnDescriptor(Bytes.toBytes(Constants.INSTAGRAM_ID));
        family.setCompressionType(Algorithm.GZ);
        tableDesc.addFamily(family);
        try {
            if (!admin.tableExists(table)) {
                admin.createTable(tableDesc);
                admin.close();
            }
        } catch (CallTimeoutException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
            throw new IOException(error);
        } catch (MasterNotRunningException | ConnectException | ZooKeeperConnectionException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
        } catch (IOException e) {
            String error = "IOException at at " + this.getClass().getName()
                    + " stack trace " + e.getMessage();
            log.error(error);
        } catch (Exception e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
            throw new Exception(error);
        }
        log.debug("Done Table with name " + tablename + " created");
    }

    public ResultScanner getResultScanner(String tableName) {

        log.info("Entered At " + this.getClass().getName() + "getResultScanner(); for table" + tableName);
        try {

            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            objResultScanner = table.getScanner(scan);


        } catch (IOException e) {

            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
            log.error("exception occured while fetching the data " + this.getClass().getName() + " exception details: " + e.getMessage());
        }
        return objResultScanner;
    }
}