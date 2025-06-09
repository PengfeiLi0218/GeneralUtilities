package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.basic.TreeNode;
import priv.lipengfei.sqlgenerator.pipeline.Expression;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.columns.numbers.NumberMapFunctions;
import tech.tablesaw.selection.Selection;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * sum(a)>10
 * @author lipengfei
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Data
public class WhereCondition extends TreeNode {
    private Expression left;
    private String op = "";
    private Expression right;

    // 三个参数默认没有函数
    public WhereCondition(String v, String o, String val){
        this.left = new Expression(v);
        this.op = o;
        this.right = new Expression(val);
    }


    public WhereCondition setOp(String op) {
        this.op = op;
        return this;
    }


    public WhereCondition setLeft(Expression left) {
        this.left = left;
        return this;
    }

    public WhereCondition setRight(Expression right) {
        this.right = right;
        return this;
    }

    public boolean check(){
        return this.getChildNodes().size()==0;
    }

    public String generateSql() throws Exception {
        if(Objects.equals(this.op, "in") || Objects.equals(this.op, "not in")){
            return String.format("%s %s ('%s')", this.left.generateSql(), this.op, String.join("' , '", this.right.getVar()));
        }else{
            return String.format("%s %s %s", this.left.generateSql(), this.op, this.right.generateSql());
        }
    }


    public Selection toSelection(Table table) throws Exception {
        StringColumn leftCol = left.toSelection(table);
        StringColumn rightCol = right.toSelection(table);


        switch (this.op){
            case ">": return leftCol.parseDouble().isGreaterThan(rightCol.parseDouble());
            case "=": return leftCol.isEqualTo(rightCol);
            case "<": return leftCol.parseDouble().isLessThan(rightCol.parseDouble());
            case "!=": return leftCol.parseDouble().isNotEqualTo(rightCol.parseDouble());
            case ">=": return leftCol.parseDouble().isGreaterThanOrEqualTo(rightCol.parseDouble());
            case "<=": return leftCol.parseDouble().isLessThanOrEqualTo(rightCol.parseDouble());
            case "in": {
                assert rightCol.allMatch(c -> !c.startsWith("$"));
                return leftCol.isIn(right.getVar());
            }
            case "not in": {
                assert rightCol.allMatch(c -> !c.startsWith("$"));
                return leftCol.isNotIn(right.getVar());
            }
        }
        return null;
    }
}
