package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/***
 * select expr
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class SelectExpression {
    // 函数名
    private String func;
    // 字段名
    private List<String> var = new ArrayList<>();
    // 别名
    private String alias;

    public SelectExpression(String v){
        var.add(v);
    }

    public SelectExpression(String f, String v){
        func = f;
        var.add(v);
    }

    public SelectExpression(String f, String v, String a){
        func = f;
        var.add(v);
        alias = a;
    }

    public SelectExpression setFunc(String func) {
        this.func = func;
        return this;
    }

    public SelectExpression setVar(String var) {
        this.var = new ArrayList<>();
        this.var.add(var);
        return this;
    }

    public SelectExpression setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getOutputName(){
        String s;
        if(func==null || func.isEmpty()) {
            // 没有函数，字段只能唯一个
            assert var.size() <= 1;
            s = String.join(",", var);
        }else
            // 有函数
            s = func + "(" + String.join(",", var) + ")";

        // 有别称用别称
        if(alias!=null && !alias.isEmpty())
            s = alias;
        return s;
    }

    @Override
    public String toString() {
        String s;
        if(func==null || func.isEmpty())
            s = String.join(",", var);
        else
            s = func + "(" + String.join(",", var) + ")";

        if(alias!=null && !alias.isEmpty())
            s = s+" as " + alias;
        return s;
    }
}
