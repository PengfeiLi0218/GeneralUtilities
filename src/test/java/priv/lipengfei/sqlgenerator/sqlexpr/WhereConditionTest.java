package priv.lipengfei.sqlgenerator.sqlexpr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.pipeline.Expression;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WhereConditionTest {
    List<WhereCondition> testcase = new ArrayList<>();
    Table table = null;
    @BeforeEach
    void setUp() {
//        new WhereCondition().setVar("$a", "$b").setOp("+").setVal("1");
        Expression expr1 = new Expression().setVar("$a", "$b").setFunc("+");
        Expression expr2 = new Expression().setVar("4");
        Expression expr3 = new Expression().setVar("lipengfei1", "lipf3");
        Expression expr4 = new Expression().setVar("$e");

        testcase.add(new WhereCondition().setRight(expr1).setLeft(expr2).setOp(">"));
        testcase.add(new WhereCondition().setRight(expr3).setLeft(expr4).setOp("in"));


        DoubleColumn a = DoubleColumn.create("a", -1.5, 2.5, -3.5, 4.5, 5.5, 6.5);
        DoubleColumn b = DoubleColumn.create("b", 0.5, 1.5, 2.5, 3.5, 4.5, 5.5);
        StringColumn c = StringColumn.create("c", "0.5", "-1.5", "2.5", "3.5", "4.5", "5.5");
        IntColumn d = IntColumn.create("d", 1,2,3,4,5,6);
        StringColumn e = StringColumn.create("e", "HELLO","world1","lipengfei1","chenyin1","PFLEE1","lipf3");


        table = Table.create("testtable").addColumns(a, b, c, d, e);
    }

    @Test
    void check() {
    }

    @Test
    void generateSql() {
        testcase.forEach(e->{
            try {
                System.out.println(e.generateSql());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @Test
    void toSelection() {
        testcase.forEach(e->{
            try {
                System.out.println(table.where(e.toSelection(table)).print());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }
}