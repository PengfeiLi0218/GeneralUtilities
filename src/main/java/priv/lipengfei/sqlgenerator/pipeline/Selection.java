package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.sqlgenerator.sqlexpr.SelectExpression;
import priv.lipengfei.utils.Utils;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/***
 * 继承至Item
 * 选择某些列，列形式的截取
 * @author lipengfei
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Selection extends Cell {
    private String distinctFlag;
    private List<String> cols = new ArrayList<>();

    public Selection setDistinctFlag(String distinctFlag) {
        this.distinctFlag = distinctFlag;
        return this;
    }

    public Selection addSelectExpression(String... expr){
        cols.addAll(List.of(expr));
        return this;
    }

    public List<SelectExpression> toSelectExpression(){
        return cols.stream().map(SelectExpression::new).toList();
    }
    // 输入 -> 输出
    @Override
    public Table execute(Table table) {
        // 有字段
        assert cols.size()>0;
        return table.selectColumns(cols.toArray(String[]::new));
    }

    // TODO: 转变为SQL操作
    @Override
    public String toString() {
        return String.format("select %s %s from %s", distinctFlag,
                String.join(",", Utils.objsToString(this.cols)),
                "temp");
    }
}
