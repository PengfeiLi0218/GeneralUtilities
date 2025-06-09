package priv.lipengfei.sqlgenerator.pipeline;

import lombok.*;
import priv.lipengfei.sqlgenerator.cells.Cell;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.table.TableSlice;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Transformation：一进一出
 * @author lipengfei
 */
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Data
public class Transformation extends Cell {
    private String type="normal"; // 滑动窗口函数或者普通函数 normal, sliding, aggregation
    private Expression expression;
    private List<String> partitionBy = new ArrayList<>();
    private List<Sort> orderBy = new ArrayList<>();

    public Transformation setType(String type) {
        this.type = type;
        return this;
    }

    public Transformation setExpression(Expression expr) {
        this.expression = expr;
        return this;
    }

    public Transformation setPartitionBy(List<String> partitionBy) {
        this.partitionBy.addAll(partitionBy);
        return this;
    }

    public Transformation setPartitionBy(String... partitionBy) {
        this.partitionBy.addAll(List.of(partitionBy));
        return this;
    }

    public Transformation addOrderBy(String orderCol, boolean asc) {
        this.orderBy.add(new Sort(orderCol, asc));
        return this;
    }

    public Transformation addOrderBy(String orderCol) {
        this.orderBy.add(new Sort(orderCol));
        return this;
    }

    @Override
    public Table execute(Table table) {
        /*
          sum(col1) as col2 ======>  col2
          sum(col1)  ========>  sum(col1)
          col1  ========>  报错，不能没有函数
         */

        assert expression!=null;
        try {
            if(Objects.equals(type, "normal")) {
                StringColumn sc = this.expression.toSelection(table);
                return table.addColumns(sc).copy();
            }else if("sliding".equals(type)) {
                Table tmp = null;
                for (TableSlice group : table.splitOn(this.partitionBy.toArray(String[]::new))) {
                    Table tp = group.asTable();
                    for (Sort c : this.orderBy) {
                        if(c.asc){
                            tp = tp.sortAscendingOn(c.getOrderby());
                        }else{
                            tp = tp.sortDescendingOn(c.getOrderby());
                        }
                    }

                    if(Objects.equals(expression.getFunc(), "row_number")) {
                        IntColumn groupRowNum = IntColumn.indexColumn(this.expression.getAlias(), group.rowCount(), 1);
                        tp = tp.addColumns(groupRowNum);
                    }else if("lag".equals(expression.getFunc())) {
                        Column<?> tmpColumn = tp.column(expression.getVar().get(0)).lag(Integer.parseInt(expression.getVar().get(1)));
                        tmpColumn.setName(this.expression.getAlias());
                        tp = tp.addColumns(tmpColumn);
                    }else if("lead".equals(expression.getFunc())) {
                        Column<?> tmpColumn = tp.column(expression.getVar().get(0)).lead(Integer.parseInt(expression.getVar().get(1)));
                        tmpColumn.setName(this.expression.getAlias());
                        tp = tp.addColumns(tmpColumn);
                    }

                    if(tmp==null){
                        tmp = tp;
                    }else {
                        tmp.append(tp);
                    }
                }
                return tmp;

            }else{
                return table;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return table;
        }
    }

    public String generateSql() throws Exception {
        String s = "";
        if(Objects.equals(type, "normal") || "aggregation".equals(type)) {
            try {
                s = this.expression.generateSql();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if(Objects.equals(type, "sliding")) {
            List<String> orderTag = orderBy.stream().map(c -> String.format("%s %s", c.getOrderby(), c.getAsc()?"asc":"desc")).toList();

            s = String.format("%s over (partition by %s order by %s)",
                    this.expression.generateSql(),
                    String.join(",", partitionBy),
                    String.join(",", orderTag)
            );
        }

        String alias = this.expression.getAlias();
        if(alias!=null && !alias.isEmpty()) {
            s += " as " + alias;
        }
        return s;
    }
}
