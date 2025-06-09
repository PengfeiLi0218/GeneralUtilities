package priv.lipengfei.sqlgenerator;

import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.pipeline.*;
import priv.lipengfei.sqlgenerator.sqlexpr.*;
import priv.lipengfei.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipelineTest {
    @Test
    void MergeTest(){
        List<WhereCondition> conditions = new ArrayList<>();
        conditions.add(new WhereCondition("a.col1", "==", "b.col2"));
//        Merge m = new Merge().setJoinType("right")
//                .setTableA("tmp1")
//                .setTableB("tmp2")
//                .setConditions(conditions);
//        m.execute();
    }

    @Test
    void PipelineFuncTest(){
//        Pipeline pipeline = new Pipeline()
//                .addItem(new Source("table1")
//                        .setTableCols(Arrays.asList("col1", "col2", "col3", "col4", "col5")))
//                .addItem(new Selection()
//                        .setDistinctFlag("distinct")
//                        .addSelectExpression(new SelectExpression("col1"))
//                        .addSelectExpression(new SelectExpression("col2"))
//                        .addSelectExpression(new SelectExpression("col3")))
//                .addItem(new Filter()
//                        .addFilterCondition(new WhereCondition("col1", ">", "100"))
//                        .addFilterCondition(new WhereCondition("col3", "==", "4"))
//                )
//                .addItem(new Filter()
//                        .addFilterCondition(new WhereCondition("col2", "<", "10")))
//                .addItem(new Aggregation()
//                        .addGrpCol("col1").addGrpCol("col2")
//                        .addAggFunc(new SelectExpression("sum", "col3", "sum_col3"))
//                        .addAggFunc(new SelectExpression("avg", "col3", "avg_col3"))
//                ).addItem(new Filter()
//                        .addFilterCondition(new WhereCondition("col1", "<", "200")));
//
//        System.out.println(pipeline.getSQLQuery());
//
//        pipeline.execute();
    }

    @Test
    void UtilsTest(){
        // 子集测试
        System.out.println(Utils.subset(
            Arrays.asList("col1", "col2", "col3"),
            Arrays.asList("col2", "col3")
        ));
    }

    @Test
    void TreeTest() {
        WhereCondition col1 = new WhereCondition("col1", ">", "100");
        WhereCondition col2 = new WhereCondition("col2", ">=", "101");
        WhereCondition col3 = new WhereCondition("col3", "<", "102");
        WhereCondition col4 = new WhereCondition("col4", "==", "103");
        WhereCondition col5 = new WhereCondition("col5", "<=", "104");
        WhereCondition col6 = new WhereCondition("col6", "!=", "105");
        WhereCondition col7 = new WhereCondition("col7", "<>", "106");
        WhereCondition col8 = new WhereCondition("col8", "==", "107");
        WhereCondition col9 = new WhereCondition("col9", "!=", "108");

//        LogisticRelation r1 = new LogisticRelation("and");
//        r1.setChildNodes(Arrays.asList(col1, col2, col3));
//
//        LogisticRelation r2 = new LogisticRelation("and");
//        r2.setChildNodes(Arrays.asList(col4, col5));
//
//        LogisticRelation r3 = new LogisticRelation("or");
//        r3.setChildNodes(Arrays.asList(col7, col8));
//
//        LogisticRelation r4 = new LogisticRelation("and");
//        r4.setChildNodes(Arrays.asList(col6, col9));
//
//        LogisticRelation r5 = new LogisticRelation("or");
//        r5.setChildNodes(Arrays.asList(r1, r2));
//
//        LogisticRelation r6 = new LogisticRelation("or");
//        r6.setChildNodes(Arrays.asList(r3, r4));
//
//        LogisticRelation r7 = new LogisticRelation("and");
//        r7.setChildNodes(Arrays.asList(r5, r6));

//        Filter filter = new Filter(Arrays.asList(col1, col2, col3, col4, col5, col6, col7, col8, col9),
//                r7);
//
//        System.out.println(filter);
//
//        System.out.println(filter.getRelationship().checkTreeNode());
//        Filter filter1 = new Filter(List.of(col1), null);
//        System.out.println(filter1);
//
//        filter.getRelationship().fitTree();
//
//        System.out.println(filter);

    }


}
