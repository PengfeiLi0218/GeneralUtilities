package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.table.TableSlice;
import tech.tablesaw.table.TableSliceGroup;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransformationTest {
    List<Transformation> testcase = new ArrayList<>();
    Table table = null;

    @BeforeEach
    void setUp() {
        testcase.add(new Transformation()
                .setType("normal")
                .setExpression(new Expression().setVar("$a", "$b").setFunc("+").setAlias("out"))

        );

        testcase.add(new Transformation()
                .setType("sliding")
                .setExpression(new Expression().setFunc("row_number").setAlias("rn"))
                .setPartitionBy("f")
                .addOrderBy("d", false)

        );

        testcase.add(new Transformation()
                .setType("sliding")
                .setExpression(new Expression().setFunc("lag").setVar("a", "1").setAlias("lag"))
                .setPartitionBy("f")
                .addOrderBy("d", false)

        );

        DoubleColumn a = DoubleColumn.create("a", -1.5, 2.5, -3.5, 4.5, 5.5, 6.5);
        DoubleColumn b = DoubleColumn.create("b", 0.5, 1.5, 2.5, 3.5, 4.5, 5.5);
        StringColumn c = StringColumn.create("c", "0.5", "-1.5", "2.5", "3.5", "4.5", "5.5");
        IntColumn d = IntColumn.create("d", 1,2,3,4,5,6);
        StringColumn e = StringColumn.create("e", "HELLO","world1","lipengfei1","chenyin1","PFLEE1","lipf3");
        StringColumn f = StringColumn.create("f", "cy","lpf","lpf","cy","cy","cy");


        table = Table.create("testtable").addColumns(a, b, c, d, e, f);
    }

    @Test
    void execute() {
        testcase.forEach(e->{
            try {
                System.out.println(e.execute(table).print());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @Test
    void toExpression() {
        testcase.forEach(e->{
            try {
                System.out.println(e.generateSql());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @Test
    void partitionTest(){
        Table t = null;
        for (TableSlice group : table.splitOn("f")) {
            Table tmp = group.asTable().sortAscendingOn("d");
            Column<?> a = tmp.doubleColumn("a").rolling(3).mean();
            tmp = tmp.addColumns(a);
            System.out.println(tmp.print());
            if(t==null){
                t = tmp;
            }else {
                t.append(tmp);
            }
        }
        assert t != null;
        System.out.println(t.print());
    }

}