package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.utils.Utils;
import tech.tablesaw.aggregate.AggregateFunction;
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
        List<String> cols = new ArrayList<>();
        List<AggregateFunction> aggfuncs = new ArrayList<>();
        List<String> selectCols = new ArrayList<>();
        for (Transformation aggCol : aggCols) {
            if("aggregation".equals(aggCol.getType())) {
                cols.add(aggCol.getExpression().getVar().get(0));
                String func = aggCol.getExpression().getFunc();

                if("sum".equalsIgnoreCase(func)) {
                    aggfuncs.add(AggregateFunctions.sum);
                    func = "Sum";
                }else if("mean".equalsIgnoreCase(func)) {
                    aggfuncs.add(AggregateFunctions.mean);
                    func = "Mean";
                }else if("count".equalsIgnoreCase(func)) {
                    aggfuncs.add(AggregateFunctions.count);
                    func = "Count";
                }else if("min".equalsIgnoreCase(func)) {
                    aggfuncs.add(AggregateFunctions.min);
                    func = "Min";
                }else if("max".equalsIgnoreCase(func)){
                    aggfuncs.add(AggregateFunctions.max);
                    func = "Max";
                }

                selectCols.add(String.format("%s [%s]", func, aggCol.getExpression().getVar().get(0)));
            }
        }

        selectCols.addAll(grpCols);
        return table.summarize(cols, aggfuncs.toArray(new AggregateFunction[0]))
                .by(grpCols.toArray(new String[0]))
                .selectColumns(selectCols.toArray(new String[0]));
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
