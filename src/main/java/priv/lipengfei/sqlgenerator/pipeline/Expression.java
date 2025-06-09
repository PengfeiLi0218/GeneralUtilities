package priv.lipengfei.sqlgenerator.pipeline;

import lombok.Data;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.columns.numbers.NumberMapFunctions;
import tech.tablesaw.selection.Selection;

import java.util.*;
import java.util.stream.Stream;

import static tech.tablesaw.api.ColumnType.*;

/**
 * @author lipengfei
 */
@Data
@NoArgsConstructor
public class Expression {
    private String func;
    private final List<String> var = new ArrayList<>();
    private String alias = UUID.randomUUID().toString();

    public Expression(String var){
        this.var.add(var);
    }

    public Expression setVar(String... var) {
        this.var.addAll(List.of(var));
        return this;
    }

    public Expression setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public Expression setFunc(String func) {
        this.func = func;
        return this;
    }

    public String generateSql() throws Exception {
        List<String> ss = this.var.stream().map(c -> {
            if (c.startsWith("$")) {
                return c.substring(1);
            } else {
                return c;
            }
        }).toList();

        if(this.func!=null && !this.func.isEmpty()) {
            if("+".equals(this.func) || "-".equals(this.func) || "*".equals(this.func) || "/".equals(this.func)){
                if(this.var.size()>1) {
                    return String.join(this.func, ss);
                }else{
                    throw new Exception("加减乘除运算参数个数必须大于1");
                }
            }
            return String.format("%s(%s)", this.func, String.join(",", ss));
        } else {
            if(ss.size()>1) {
                return "(" + String.join(",", ss) + ")";
            }else{
                return ss.get(0);
            }
        }
    }

    public DoubleColumn readDoubleColumn(String colName, Table table){
        int cnt = table.rowCount();
        if (colName.startsWith("$")) {
            colName = colName.substring(1);
            ColumnType type = table.column(colName).type();
            if (INTEGER.equals(type)) {
                return table.intColumn(colName).asDoubleColumn().setName(this.alias);
            } else if (DOUBLE.equals(type)) {
                return table.doubleColumn(colName).setName(this.alias);
            } else if (STRING.equals(type)) {
                return table.stringColumn(colName).parseDouble().setName(this.alias);
            } else if (FLOAT.equals(type)) {
                return table.floatColumn(colName).asDoubleColumn().setName(this.alias);
            } else if (SHORT.equals(type)) {
                return table.shortColumn(colName).asDoubleColumn().setName(this.alias);
            } else {
                return null;
            }
        } else {
            return DoubleColumn.create(this.alias, cnt).fillWith(Double.parseDouble(colName));
        }
    }

    public StringColumn readStringColumn(String colName, Table table){
        int cnt = table.rowCount();
        if (colName.startsWith("$")) {
            colName = colName.substring(1);
            ColumnType type = table.column(colName).type();
            if (INTEGER.equals(type)) {
                return table.intColumn(colName).asStringColumn().setName(this.alias);
            } else if (DOUBLE.equals(type)) {
                return table.doubleColumn(colName).asStringColumn().setName(this.alias);
            } else if (STRING.equals(type)) {
                return table.stringColumn(colName).setName(this.alias);
            } else if (FLOAT.equals(type)) {
                return table.floatColumn(colName).asStringColumn().setName(this.alias);
            } else if (SHORT.equals(type)) {
                return table.shortColumn(colName).asStringColumn().setName(this.alias);
            } else {
                return null;
            }
        } else {
            String[] arr = new String[cnt];
            Arrays.fill(arr, colName);
            return StringColumn.create(this.getAlias(), arr);
        }
    }

    public StringColumn toSelection(Table table) throws Exception {
        StringColumn result = null;
        if(this.func!=null && !this.func.isEmpty()) {
            if("abs".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readDoubleColumn(var.get(0), table)).abs().asStringColumn();
            } else if("sqrt".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readDoubleColumn(var.get(0), table)).sqrt().asStringColumn();
            } else if("round".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readDoubleColumn(var.get(0), table)).round().asStringColumn();
            } else if("+".equals(this.func) || "-".equals(this.func) || "*".equals(this.func) || "/".equals(this.func)) {
                List<DoubleColumn> result1 = getVar().stream().map(c -> this.readDoubleColumn(c, table)).toList();

                DoubleColumn init = null;
                for (DoubleColumn c : result1) {
                    if (init == null) {
                        init = c;
                    } else if ("+".equals(this.func)) {
                        init = init.add(c);
                    } else if ("-".equals(this.func)) {
                        init = init.subtract(c);
                    } else if ("*".equals(this.func)) {
                        init = init.multiply(c);
                    } else if ("/".equals(this.func)) {
                        init = init.divide(c);
                    }
                }
                assert init != null;
                return init.asStringColumn();
            } else if("concat".equalsIgnoreCase(this.func)){
                List<StringColumn> result1 = getVar().stream().map(c -> this.readStringColumn(c, table)).toList();
                StringColumn init = null;
                for (StringColumn c : result1) {
                    if (init == null) {
                        init = c;
                    } else if ("concat".equals(this.func)) {
                        init = init.concatenate(c);
                    }
                }
                return init;
            } else if("upper".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readStringColumn(var.get(0), table)).upperCase().asStringColumn();
            } else if("lower".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readStringColumn(var.get(0), table)).lowerCase().asStringColumn();
            } else if("length".equalsIgnoreCase(this.func)){
                return Objects.requireNonNull(this.readStringColumn(var.get(0), table)).length().asStringColumn();
            } else if("substring".equalsIgnoreCase(this.func)){
                // TODO 需要测试
                StringColumn sc = this.readStringColumn(var.get(0), table);
                assert sc != null;
                if(var.size()==3) {
                    return Objects.requireNonNull(sc.substring(Integer.parseInt(var.get(1)), Integer.parseInt(var.get(2))).asStringColumn());
                }else if(var.size()==2){
                    return Objects.requireNonNull(sc.substring(Integer.parseInt(var.get(1))).asStringColumn());
                }else{
                    throw new Exception("substring函数参数个数不正确");
                }

            }

        } else {
            String s = getVar().get(0);
            return Objects.requireNonNull(this.readStringColumn(s, table));
        }

        return result;
    }
}
