package com.yourcompany.component.ss.emitter.hbase;

public class PartitionTable {
    private Integer tableToTime;
    private Integer tableFromTime;
    private Integer searchToTime;
    private Integer searchFromTime;
    private Integer configureTime;
    private Integer rowId;

    public Integer getTableToTime() {
        return tableToTime;
    }

    public void setTableToTime(Integer tableToTime) {
        this.tableToTime = tableToTime;
    }

    public Integer getTableFromTime() {
        return tableFromTime;
    }

    public void setTableFromTime(Integer tableFromTime) {
        this.tableFromTime = tableFromTime;
    }

    public Integer getSearchToTime() {
        return searchToTime;
    }

    public void setSearchToTime(Integer searchToTime) {
        this.searchToTime = searchToTime;
    }

    public Integer getSearchFromTime() {
        return searchFromTime;
    }

    public void setSearchFromTime(Integer searchFromTime) {
        this.searchFromTime = searchFromTime;
    }

    public Integer getConfigureTime() {
        return configureTime;
    }

    public void setConfigureTime(Integer configureTime) {
        this.configureTime = configureTime;
    }

    public Integer getRowId() {
        return rowId;
    }

    public void setRowId(Integer rowId) {
        this.rowId = rowId;
    }

}
