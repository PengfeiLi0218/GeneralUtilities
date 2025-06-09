package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.cells.Edge;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;

import java.util.List;

class TopologicalTreeTest {
    private static final TopologicalTree tree = new TopologicalTree();

    @BeforeEach
    void setUp() {
        Source table = new Source("业扩工单");
        Transformation transformer1 = new Transformation().setType("sliding")
                .setPartitionBy("wo_id")
                .addOrderBy("task_num", false)
                .setExpression(new Expression().setFunc("row_number").setAlias("rn"));

        Transformation transformer2 = new Transformation().setType("sliding")
                .setPartitionBy("instance_id", "province_code")
                .addOrderBy("task_num", false)
                .setExpression(new Expression().setFunc("row_number").setAlias("rn"));


        Transformation transformer3 = new Transformation().setType("normal")
                .setExpression(new Expression("tw_province_code").setAlias("province"));

        Transformation transformer4 = new Transformation().setType("normal")
                .setExpression(new Expression("03").setAlias("province"));


        Transformation transformer5 = new Transformation().setType("normal")
                .setExpression(new Expression("tw_province_code").setAlias("province"));

        Filter filter1 = new Filter().
                setFilterConditions(
//                        new WhereCondition().setVar("wo_status_code").setOp("in").setVal("3", "4"),
//                        new WhereCondition("operated_time", ">=", "{now_date - 1month}"),
//                        new WhereCondition("operated_time", "<=", "{now_date}"),
//                        new WhereCondition().setVar("busi_type_no").setOp("in")
//                                .setVal("F-CSG-MK0513-03",
//                                        "F-CSG-MK0513-02",
//                                        "F-CSG-MK0513-01",
//                                        "F-CSG-MK0512-01"),
                        new WhereCondition("rn", "=", "1")
                );

        Filter filter2 = new Filter().setFilterConditions(
                new WhereCondition("tw_province_code", "<>", "03")
        );

        Source table2 = new Source("工单处理信息历史表");
        Source table3 = new Source("作废工单");
        Source table4 = new Source("作废工单（广东）");
        Source table5 = new Source("子任务流程表");
        Source table6 = new Source("代码编码表");


        MergeItem merge1 = new MergeItem().setJoinType("left")
                .setConditions(new Filter().setFilterConditions(
                        new WhereCondition("instance_id", "=", "instance_id"),
                        new WhereCondition("tw_province_code", "=", "tw_province_code"),
                        new WhereCondition("rn", "=", "1")
                ));

        MergeItem merge2 = new MergeItem().setJoinType("union");

        MergeItem merge3 = new MergeItem().setJoinType("left")
                .setConditions(new Filter().setFilterConditions(
                        new WhereCondition("wo_no", "=", "old_wo_no"),
                        new WhereCondition("tw_province_code", "=", "province")
                ));

        Selection selection = new Selection().addSelectExpression("wo_no", "rn");


        Edge edge1 = new Edge().setSourceId(table.getId()).setTargetId(transformer1.getId());
        Edge edge2 = new Edge().setSourceId(transformer1.getId()).setTargetId(transformer5.getId());
        Edge edge14 = new Edge().setSourceId(transformer5.getId()).setTargetId(filter1.getId());
        Edge edge3 = new Edge().setSourceId(table2.getId()).setTargetId(transformer2.getId());
        Edge edge4 = new Edge().setSourceId(transformer2.getId()).setTargetId(merge1.getId());
        Edge edge5 = new Edge().setSourceId(filter1.getId()).setTargetId(merge1.getId());
        Edge edge6 = new Edge().setSourceId(table3.getId()).setTargetId(transformer3.getId());
        Edge edge7 = new Edge().setSourceId(table4.getId()).setTargetId(transformer4.getId());
        Edge edge8 = new Edge().setSourceId(transformer3.getId()).setTargetId(filter2.getId());
        Edge edge9 = new Edge().setSourceId(filter2.getId()).setTargetId(merge2.getId());
        Edge edge10 = new Edge().setSourceId(transformer4.getId()).setTargetId(merge2.getId());
        Edge edge11 = new Edge().setSourceId(merge1.getId()).setTargetId(merge3.getId());
        Edge edge12 = new Edge().setSourceId(merge2.getId()).setTargetId(merge3.getId());
        Edge edge13 = new Edge().setSourceId(merge3.getId()).setTargetId(selection.getId());

        tree.addCells(table, table2, table3, table4, table5, table6,
                transformer1, transformer2, transformer3, transformer4,transformer5,
                filter1, filter2,
                merge1, merge2, merge3,
                selection,
                edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9, edge10, edge11, edge12, edge13, edge14
        );
    }

    @Test
    void getPipelines() {
        System.out.println(tree.getItems().values());
        List<Pipeline> pipelines = tree.getPipelines();
        System.out.println(pipelines.size());
        for (Pipeline pipeline : pipelines) {
            System.out.println(pipeline.getSQLQuery());
        }
    }

    @Test
    void getMergeCells() {
        System.out.println(tree.getItems());
    }

    @Test
    void getInputs() {
        System.out.println(tree);
        System.out.println(tree.getInputs());
    }

    @Test
    void getOutputs() {
    }
}