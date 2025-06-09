package priv.lipengfei.sqlgenerator.pipeline;

import com.ibm.icu.impl.UResource;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.cells.Cell;
import tech.tablesaw.api.Table;

import java.util.Arrays;
import java.util.List;

/**
 * @author lipengfei
 */
@NoArgsConstructor
@Getter
public class Source extends Cell {
    private String table;
    private List<String> tableCols;

    public Source(String table){
        this.table = table;
    }

    // TODO: 获得数据结构
    public List<String> setTableColsFromDB(){
        return List.of();
    }

    public Source setTableCols(List<String> tableCols){
        this.tableCols = tableCols;
        return this;
    }

    @Override
    public Table execute(Table table) {
        return table.selectColumns(tableCols.toArray(new String[0]));
    }
}
