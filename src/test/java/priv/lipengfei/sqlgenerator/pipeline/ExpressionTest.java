package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExpressionTest {
    /**
     *
     */
    List<Expression> testcase = new ArrayList<>();
    Table table = null;
    @BeforeEach
    void setUp() {
        // 数字计算
        testcase.add(new Expression("$a"));
        testcase.add(new Expression().setVar("$a", "$b").setFunc("+"));
        testcase.add(new Expression().setVar("$a", "10").setFunc("+"));
        testcase.add(new Expression().setVar("$c", "$a").setFunc("+"));
        testcase.add(new Expression().setVar("10"));
        testcase.add(new Expression().setVar("$a").setFunc("abs"));
        testcase.add(new Expression().setVar("$c").setFunc("abs"));
        testcase.add(new Expression().setVar("$a").setFunc("sqrt"));
        testcase.add(new Expression().setVar("$c").setFunc("sqrt"));
        testcase.add(new Expression().setVar("$a").setFunc("round"));
        testcase.add(new Expression().setVar("$c").setFunc("round"));


        // 字符串计算
        testcase.add(new Expression().setVar("$e", "$e").setFunc("concat"));
        testcase.add(new Expression().setVar("$e").setFunc("upper"));
        testcase.add(new Expression().setVar("$e").setFunc("lower"));
        testcase.add(new Expression().setVar("$e").setFunc("length"));
        testcase.add(new Expression().setVar("$e", "1").setFunc("substring"));
        testcase.add(new Expression().setVar("$e", "1", "3").setFunc("substring"));


        DoubleColumn a = DoubleColumn.create("a", -1.5, 2.5, -3.5, 4.5, 5.5, 6.5);
        DoubleColumn b = DoubleColumn.create("b", 0.5, 1.5, 2.5, 3.5, 4.5, 5.5);
        StringColumn c = StringColumn.create("c", "0.5", "-1.5", "2.5", "3.5", "4.5", "5.5");
        IntColumn d = IntColumn.create("d", 1,2,3,4,5,6);
        StringColumn e = StringColumn.create("e", "HELLO","world1","lipengfei1","chenyin1","PFLEE1","lipf3");


        table = Table.create("testtable").addColumns(a, b, c, d, e);

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
                System.out.println(e.toSelection(table).print());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }
}