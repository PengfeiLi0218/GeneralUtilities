package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.sqlexpr.SelectExpression;
import priv.lipengfei.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Aggregation extends Item {
    private List<String> grpCols = new ArrayList<>();
    private List<SelectExpression> aggCols = new ArrayList<>();

    public Aggregation addAggFunc(SelectExpression af){
        this.aggCols.add(af);
        return this;
    }

    public Aggregation addGrpCol(String col){
        this.grpCols.add(col);
        return this;
    }

    // 获得聚合函数的字段
    private List<String> getAggFuncVars(){
        return this.aggCols.parallelStream().flatMap(item -> item.getVar().stream())
                .distinct().collect(Collectors.toList());
    }

    @Override
    public DataTable execute(DataTable table) {
        // 判断grpCol和aggCols的字段是table数据表的子集
        assert Utils.subset(table.getTableCols(), grpCols);
        assert Utils.subset(table.getTableCols(), getAggFuncVars());

        return new DataTable()
                .setTableCols(grpCols)
                .extendTableCols(
                    aggCols.stream().map(SelectExpression::getOutputName).collect(Collectors.toList())
                );
    }

    @Override
    public String toString() {
        return String.format("select %s,%s from %s group by %s",
                String.join(",", this.grpCols),
                String.join(",", Utils.objsToString(this.aggCols)),
                this.name,
                String.join(",", this.grpCols)
        );
    }

}
