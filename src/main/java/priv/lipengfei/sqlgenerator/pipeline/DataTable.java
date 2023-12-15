package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

// 数据结构
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class DataTable {
    // 表名
    private String tableName;
    // 字段名
    private List<String> tableCols = new ArrayList<>();

    public DataTable(String tn){
        this.tableName = tn;
    }

    public DataTable setTableName(String tn){
        this.tableName = tn;
        return this;
    }

    public DataTable setTableCols(List<String> tableCols) {
        this.tableCols = new ArrayList<>(tableCols);
        return this;
    }

    public DataTable extendTableCols(List<String> tableCols) {
        this.tableCols.addAll(tableCols);
        return this;
    }

    public DataTable addTableCol(String col) {
        this.tableCols.add(col);
        return this;
    }

    @Override
    public String toString() {
        try {
            return String.join(",", tableCols);
        }catch (Exception e){
            return null;
        }
    }


}
