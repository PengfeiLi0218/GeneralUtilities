package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import priv.lipengfei.sqlgenerator.pipeline.Filter;
import priv.lipengfei.sqlgenerator.pipeline.Transformation;
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
 * @author lipengfei
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Slf4j
public class SqlQuery {
    private String distinctFlag="";
    private List<String> selectExprs = new ArrayList<>(); // 查询语句，一个SelctExpr是一个字段
    private List<Transformation> transformExprs = new ArrayList<>(); // 查询语句，一个SelctExpr是一个字段
    private String tableReferences = "";
    private List<Filter> whereConditions = new ArrayList<>();
    private GroupCondition groupCondition;
    private int limitNo=-1;

    public SqlQuery addSelectExpr(String expr){
        this.selectExprs.add(expr);
        return this;
    }

    public SqlQuery addSelectExprs(List<String> selectExprs){
        this.selectExprs.addAll(selectExprs);
        return this;
    }

    public SqlQuery addTable(String table){
        if(this.tableReferences==null || this.tableReferences.isEmpty()) {
            this.tableReferences = table;
        }else{
            log.error("表已经存在了");
        }
        return this;
    }

    public SqlQuery addFilter(Filter wc){
        if(groupCondition==null) {
            this.whereConditions.add(wc);
        }else{
            log.error("已经存在Group条件了，不能添加Filter条件，要进行嵌套");
        }
        return this;
    }

    public SqlQuery setLimitNo(int limitNo) {
        this.limitNo = limitNo;
        return this;
    }

    @Override
    public String toString() {
        String exprs = "";
        if(groupCondition != null) {
            exprs = String.join(",", groupCondition.getCols());
        } else if(selectExprs.isEmpty()) {
            exprs = "*";
        } else {
            exprs = String.join(",", selectExprs);
        }

        if(transformExprs.size()>0) {
            exprs = exprs + " , " + String.join(",", Utils.objsToString(transformExprs));
        }

        String s =  String.format("SELECT %s %s FROM %s",
                this.distinctFlag,
                exprs,
                String.join(",", tableReferences)
        );

        // 如果存在Filter条件
        if(whereConditions!=null && !whereConditions.isEmpty()) {
            s += (" WHERE "+String.join(" AND ",
                    whereConditions.parallelStream().map(c -> {
                        try {
                            return c.enumRoot();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }).toList()));
        }

        // 如果存在Group条件
        if(groupCondition!=null && !groupCondition.getCols().isEmpty()) {
            s += (" " + groupCondition);
        }

        // 如果存在LIMIT
        if(limitNo>0) {
            s += String.format(" LIMIT %d", limitNo);
        }
        return s;
    }

    public SqlQuery addTransformExpr(Transformation... expression) {
        this.transformExprs.addAll(Arrays.asList(expression));
        return this;
    }

    public SqlQuery setDistinctFlag(String distinct) {
        this.distinctFlag = distinct;
        return this;
    }

    public SqlQuery addGroupCondition(GroupCondition extendCols) {
        if(groupCondition==null) {
            this.groupCondition = extendCols;
        }else{
            log.error("已经存在Group条件了，不能添加Group条件，要进行嵌套");
        }
        return this;
    }
}
