package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/***
 * select expr
 * @author lipengfei
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Data
public class SelectExpression {
    private String type = "normal";
    // 函数名
    private String func;
    // 字段名
    private List<String> var = new ArrayList<>();
    // 别名
    private String alias;

    private List<String> partitionBy = new ArrayList<>();
    private List<String> orderBy = new ArrayList<>();
    private List<String> ascTag = new ArrayList<>();

    public SelectExpression addVar(String... var) {
        this.var.addAll(List.of(var));
        return this;
    }

    public SelectExpression setType(String type) {
        this.type = type;
        return this;
    }

    public SelectExpression addParatitionBy(String... partitionBy) {
        this.partitionBy.addAll(List.of(partitionBy));
        return this;
    }

    public SelectExpression addOrderBy(String... orderBy) {
        this.orderBy.addAll(List.of(orderBy));
        return this;
    }

    public SelectExpression addAscTag(String... ascTag) {
        this.ascTag.addAll(List.of(ascTag));
        return this;
    }

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
        {
            s = func + "(" + String.join(",", var) + ")";
        }

        // 有别称用别称
        if(alias!=null && !alias.isEmpty()) {
            s = alias;
        }
        return s;
    }

    @Override
    public String toString() {
//        assert func!=null && !func.isEmpty();
        String s = "";
        if(Objects.equals(type, "normal")) {
            if(func==null || func.isEmpty()){
                s = String.join(",", var);
            }else {
                s = String.format("%s(%s)", func,
                        String.join(",", var));
            }


        } else if(Objects.equals(type, "sliding")) {
            assert ascTag.size() == orderBy.size();

            int size = ascTag.size();
            List<String> orderTag = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                orderTag.add(orderBy.get(i)+" "+ascTag.get(i));
            }

            s = String.format("%s(%s) over (partition by %s order by %s)",
                    func, String.join(",", var),
                    String.join(",", partitionBy),
                    String.join(",", orderTag)
            );
        }

        if(alias!=null && !alias.isEmpty()) {
            s += " as " + alias;
        }
        return s;
    }
}
