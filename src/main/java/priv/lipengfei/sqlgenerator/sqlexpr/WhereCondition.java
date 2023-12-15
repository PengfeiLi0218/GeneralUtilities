package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.basic.TreeNode;

/***
 * sum(a)>10
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class WhereCondition extends TreeNode {
    private String func;
    private String var;
    private String op;
    private String val;

    // 三个参数默认没有函数
    public WhereCondition(String v, String o, String val){
        this.var = v;
        this.op = o;
        this.val = val;
    }

    public WhereCondition setFunc(String func) {
        this.func = func;
        return this;
    }
    public WhereCondition setOp(String op) {
        this.op = op;
        return this;
    }

    public WhereCondition setVar(String var) {
        this.var = var;
        return this;
    }

    public WhereCondition setAlias(String val) {
        this.val = val;
        return this;
    }

    public boolean check(){
        return this.getChildNodes().size()==0;
    }

    @Override
    public String toString() {
        if(this.func!=null && !this.func.isEmpty())
            return String.format("%s(%s) %s %s", this.func, this.var, this.op, this.val);
        else
            return String.format("%s %s %s", this.var, this.op, this.val);
    }
}
