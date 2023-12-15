package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.sqlexpr.SelectExpression;
import priv.lipengfei.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/***
 * 继承至Item
 * 选择某些列，列形式的截取
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Selection extends Item {
    private String distinctFlag;
    private List<SelectExpression> cols = new ArrayList<>();

    public Selection setDistinctFlag(String distinctFlag) {
        this.distinctFlag = distinctFlag;
        return this;
    }

    public Selection addSelectExpression(SelectExpression expr){
        cols.add(expr);
        return this;
    }

    public Selection extendSelectExpressions(List<SelectExpression> exprs){
        cols.addAll(exprs);
        return this;
    }

    // 输入 -> 输出
    @Override
    public DataTable execute(DataTable table) {
        // 有字段
        assert cols.size()>0;
        // TODO：字段在数据结构中

        return new DataTable(table.getTableName())
                .setTableCols(cols.parallelStream().map(SelectExpression::getOutputName).collect(Collectors.toList()));
    }

    // TODO: 转变为SQL操作
    @Override
    public String toString() {
        return String.format("select %s %s from %s", distinctFlag,
                String.join(",", Utils.objsToString(this.cols)),
                this.name);
    }
}
