package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.sqlexpr.LogisticRelation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FilterTest {
    List<Filter> testcase = new ArrayList<>();
    Table table = null;
    @BeforeEach
    void setUp() {
        testcase.add(new Filter(
            new WhereCondition()
                    .setLeft(new Expression().setVar("$a", "$b").setFunc("+"))
                    .setRight(new Expression().setVar("4"))
                    .setOp(">")
                ,
            new WhereCondition()
                    .setLeft(new Expression().setVar("$e"))
                    .setRight(new Expression().setVar("lipengfei1", "lipf3"))
                    .setOp("in")
        ));

        LogisticRelation lr1 = new LogisticRelation("or")
                .setChildNodes(
                        new WhereCondition().setLeft(new Expression().setVar("$a", "$b").setFunc("+")).setOp(">").setRight(new Expression().setVar("4")),
                        new WhereCondition().setLeft(new Expression().setVar("$e")).setOp("in").setRight(new Expression().setVar("lipengfei1", "lipf3"))
                );

        LogisticRelation lr2 = new LogisticRelation("or")
                .setChildNodes(
                        new WhereCondition().setLeft(new Expression().setVar("$a", "$b").setFunc("-")).setOp("<").setRight(new Expression().setVar("3")),
                        new WhereCondition().setLeft(new Expression().setVar("$e")).setOp("not in").setRight(new Expression().setVar("lipengfei1", "lipf3"))
                );

        testcase.add(new Filter().setRelationship(
                new LogisticRelation("and").setChildNodes(
                    lr1,lr2,new WhereCondition().setLeft(new Expression().setVar("$a", "$b").setFunc("*")).setOp(">").setRight(new Expression().setVar("4"))
                )
        ));

        DoubleColumn a = DoubleColumn.create("a", -1.5, 2.5, -3.5, 4.5, 5.5, 6.5);
        DoubleColumn b = DoubleColumn.create("b", 0.5, 1.5, 2.5, 3.5, 4.5, 5.5);
        StringColumn c = StringColumn.create("c", "0.5", "-1.5", "2.5", "3.5", "4.5", "5.5");
        IntColumn d = IntColumn.create("d", 1,2,3,4,5,6);
        StringColumn e = StringColumn.create("e", "HELLO","world1","lipengfei1","chenyin1","PFLEE1","lipf3");


        table = Table.create("testtable").addColumns(a, b, c, d, e);
    }

    @Test
    void toSelection() {
        testcase.forEach(e->{
            try {
                System.out.println(e.execute(table).print());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
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
}