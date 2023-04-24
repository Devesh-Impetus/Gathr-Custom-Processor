package com.yourcompany.component.ss.emitter.hbase;

import com.yourcompany.component.ss.common.Constants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionManager {

    static Logger log = LoggerFactory.getLogger(PartitionManager.class);
    private static PartitionManager partitionmanagerReader;
    private static Map<String, Set<PartitionTable>> dbTosetOfPartition;

    private PartitionManager() {

    }

    public static synchronized PartitionManager getInstance() {

        if (partitionmanagerReader == null) {
            partitionmanagerReader = new PartitionManager();
            partitionmanagerReader.dbTosetOfPartition = getFingerPrintingTableMapping();

        }
        return partitionmanagerReader;
    }

    public static Map<String, Set<PartitionTable>> getFingerPrintingTableMapping() {
        Map<String, Set<PartitionTable>> dbTosetOfPartition = new HashMap<String, Set<PartitionTable>>();
        Set<PartitionTable> setOfTableMap = new HashSet<PartitionTable>();

        HBaseAccessLayer hbaseDataAccess;
        ResultScanner objResultScanner;
        String tableName = Constants.PARTITION_TABLE_NAME;


        hbaseDataAccess = new HBaseAccessLayer();
        objResultScanner = hbaseDataAccess.getResultScanner(tableName);
        PartitionTable tableMap = null;

        try {
            if (objResultScanner != null) {
                for (Result result = objResultScanner.next(); result != null; result = objResultScanner.next()) {
                    tableMap = new PartitionTable();

                    tableMap.setRowId(Integer.parseInt(Bytes.toString(result.getRow())));
                    tableMap.setTableFromTime(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.PART_COL_QULIFIER), Bytes.toBytes(Constants.PART_FROM_TIME)))));
                    tableMap.setTableToTime(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.PART_COL_QULIFIER), Bytes.toBytes(Constants.PART_TO_TIME)))));
                    tableMap.setSearchFromTime(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.PART_COL_QULIFIER), Bytes.toBytes(Constants.SEARCH_FROM_TIME)))));
                    tableMap.setSearchToTime(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.PART_COL_QULIFIER), Bytes.toBytes(Constants.SEARCH_TO_TIME)))));
                    tableMap.setConfigureTime(Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes(Constants.PART_COL_QULIFIER), Bytes.toBytes(Constants.CONFIGURE_TIME)))));
                    setOfTableMap.add(tableMap);
                }
                objResultScanner.close();
            }
            dbTosetOfPartition.put(Constants.PARTITION_DB_NAME, setOfTableMap);
        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error = e.toString();
            log.error(error);
        }

        return dbTosetOfPartition;
    }

    public Map<String, Set<PartitionTable>> getSetOfTableMap() {
        return this.getInstance().dbTosetOfPartition;
    }

    public String getTableName(Set<PartitionTable> setOfPatitionTableObjects, Long TimeSpan) throws Exception {
        String tableName = null;

        for (PartitionTable objTableMap : setOfPatitionTableObjects) {
            if (objTableMap.getTableFromTime() <= TimeSpan) {
                if (TimeSpan <= objTableMap.getTableToTime() || objTableMap.getTableToTime() == -1) {
                    Long starttime;
                    Long Temp = TimeSpan;
                    Temp = Temp / objTableMap.getConfigureTime();
                    Temp = Temp * objTableMap.getConfigureTime();
                    starttime = Temp;
                    Temp = Temp + objTableMap.getConfigureTime();
                    tableName = "_" + starttime + "_" + Temp;
                    break;
                }
            }
        }
        if (tableName == null) {
            throw new Exception("cant Calculate table Name");
        }

        return tableName;
    }
}
