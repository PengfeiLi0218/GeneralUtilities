package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.pipeline.Filter;
import priv.lipengfei.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 * 单表查询SQl语句
 * SELECT [ALL | DISTINCT] select_expr, select_expr, ...
 * FROM table_reference
 * [WHERE where_condition]
 * [GROUP BY col_list [HAVING condition]]
 * [CLUSTER BY col_list
 *   | [DISTRIBUTE BY col_list] [SORT BY| ORDER BY col_list]
 * ]
 * [LIMIT number]
 *
 * Select
 * Filter
 * Group Having
 *
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
public class SQLQuery {
    private String distinctFlag="";
    private List<SelectExpression> selectExprs = new ArrayList<>(); // 查询语句，一个SelctExpr是一个字段
    private List<String> tableReferences = new ArrayList<>();
    private List<Filter> whereConditions = new ArrayList<>();
    private GroupCondition groupCondition;
    private int limitNo=-1;

    public SQLQuery setGroupCondition(GroupCondition groupCondition) {
        this.groupCondition = groupCondition;
        return this;
    }

    public SQLQuery setDistinctFlag(String distinctFlag) {
        this.distinctFlag = distinctFlag;
        return this;
    }

    public SQLQuery setSelectExprs(List<SelectExpression> selectExprs) {
        this.selectExprs = selectExprs;
        return this;
    }

    public SQLQuery addSelectExpr(SelectExpression... expr){
        this.selectExprs.addAll(Arrays.asList(expr));
        return this;
    }

    public SQLQuery extendSelectExprs(List<SelectExpression> selectExprs){
        this.selectExprs.addAll(selectExprs);
        return this;
    }

    public SQLQuery extendSelectExprsString(List<String> selectExprs){
        selectExprs.parallelStream().forEach(item -> {
            this.selectExprs.add(new SelectExpression(item));
        });
        return this;
    }

    public SQLQuery setTableReference(List<String> table_reference) {
        this.tableReferences = table_reference;
        return this;
    }

    public SQLQuery addTable(String...table){
        this.tableReferences.addAll(Arrays.asList(table));
        return this;
    }

    public SQLQuery addFilter(Filter... wc){
        this.whereConditions.addAll(Arrays.asList(wc));
        return this;
    }

    public SQLQuery extendFilter(List<Filter> wc){
        this.whereConditions.addAll(wc);
        return this;
    }

    public SQLQuery setWhereConditions(List<Filter> whereConditions) {
        this.whereConditions = new ArrayList<>(whereConditions);
        return this;
    }

    public SQLQuery setLimitNo(int limitNo) {
        this.limitNo = limitNo;
        return this;
    }

    @Override
    public String toString() {
        String exprs = null;
        if(selectExprs.isEmpty())
            exprs = "*";
        else
            exprs = String.join(",", Utils.objsToString(selectExprs));
        String s =  String.format("SELECT %s %s FROM %s",
                this.distinctFlag,
                exprs,
                String.join(",", tableReferences)
        );

        // 如果存在Filter条件
        if(whereConditions!=null && !whereConditions.isEmpty())
            s += (" WHERE "+String.join(" AND ",
                    whereConditions.parallelStream().map(Filter::enumRoot).toList()));

        // 如果存在Group条件
        if(groupCondition!=null && !groupCondition.getCols().isEmpty())
            s += (" " + groupCondition);

        // 如果存在LIMIT
        if(limitNo>0)
            s += String.format(" LIMIT %d", limitNo);
        return s;
    }
}
