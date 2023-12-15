package priv.lipengfei.sqlgenerator.pipeline;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@Getter
public class Source extends Item {
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
    public DataTable execute(DataTable table) {
        return new DataTable(this.table)
                .setTableCols(this.tableCols);
    }
}
