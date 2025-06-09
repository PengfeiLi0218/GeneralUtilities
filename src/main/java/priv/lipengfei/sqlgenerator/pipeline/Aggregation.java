package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.utils.Utils;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;


/**
 * @author lipengfei
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Aggregation extends Cell {
    private List<String> grpCols = new ArrayList<>();
    private List<Transformation> aggCols = new ArrayList<>();

    public Aggregation addAggFuncs(Transformation... af){
        this.aggCols.addAll(List.of(af));
        return this;
    }

    public Aggregation addGrpCols(String... col){
        this.grpCols.addAll(List.of(col));
        return this;
    }

    @Override
    public Table execute(Table table) {
        // 判断grpCol和aggCols的字段是table数据表的子集

        for (Transformation aggCol : aggCols) {
            String func = aggCol.getExpression().getFunc();
            String col = aggCol.getExpression().getVar().get(0);
            if("mean".equals(func) || "avg".equals(func)){
//                AggregateFunctions.mean(table.doubleColumn(col))
//                table.summarize(col, mean).by(this.grpCols.toArray(new String[0]));
            }
        }
        return table;
    }

    @Override
    public String toString() {
        return String.format("select %s,%s from %s group by %s",
                String.join(",", this.grpCols),
                String.join(",", Utils.objsToString(this.aggCols)),
                "tmp",
                String.join(",", this.grpCols)
        );
    }
}
