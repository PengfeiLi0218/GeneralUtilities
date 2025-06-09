package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.cells.*;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import tech.tablesaw.api.Table;
import java.util.HashMap;
import java.util.Map;
import static tech.tablesaw.aggregate.AggregateFunctions.count;

class PipelineTest {
    private static Pipeline pipeline = new Pipeline();
    private static Map<String, Cell> cellMap = new HashMap<>();

    @BeforeAll
    static void setUp() {
        Source table = new Source("业扩工单");
        Transformation transformer1 = new Transformation().setType("sliding")
                .setPartitionBy("wo_id")
                .addOrderBy("task_num", false)
                .setExpression(new Expression().setFunc("row_number").setAlias("rn"));

        Filter filter = new Filter(
//                        new WhereCondition().setVar("wo_status_code").setOp("in").setVal("3", "4"),
//                        new WhereCondition("operated_time", ">=", "{now_date - 1month}"),
//                        new WhereCondition().setVar("busi_type_no").setOp("in")
//                                .setVal("F-CSG-MK0513-03",
//                                        "F-CSG-MK0513-02",
//                                        "F-CSG-MK0513-01",
//                                        "F-CSG-MK0512-01")
                );

        Filter filter2 = new Filter(new WhereCondition("operated_time", "<=", "{now_date}"));

        Filter filter3 = new Filter(new WhereCondition("rn", "=", "1"));

        Transformation transformer2 = new Transformation().setType("normal")
                .setExpression(new Expression("tw_province_code").setAlias("province"));

        Aggregation aggregation = new Aggregation().addGrpCols("wo_id")
                .addAggFuncs(
                        new Transformation().setType("normal").setExpression(new Expression().setFunc("sum").setVar("rn")),
                        new Transformation().setType("normal").setExpression(new Expression().setFunc("count").setVar("rn"))
                );

        pipeline.addNodes(table, transformer1, filter, filter2, filter3, aggregation, transformer2);
    }

    @Test
    void addCells() {
//        Transformation add = new Transformation().setFunction("add");
//        Transformation minus = new Transformation().setFunction("minus");
//        Transformation multiply = new Transformation().setFunction("multiply");
//        pipeline.addNodes(cellMap.get("table"),
//                cellMap.get("filter"),
//                cellMap.get("transformer1"),
//                cellMap.get("edge2"),
//                cellMap.get("edge1"),
//                add, minus, multiply);
        System.out.println(pipeline);
//        pipeline.addNodes(new Edge().setSourceId(cellMap.get("filter").getId()).setTargetId(minus.getId()));
        System.out.println(pipeline.getSQLQuery());
    }

    @Test
    void mergePipeline() {
        Table t = Table.read().file("/Users/lipengfei/Downloads/数字服务平台（服务运营）V1.0-Bug.csv");
//        System.out.println(t);
        t = t.where(t.stringColumn("所属模块").isEqualTo("/产品生态(#287)"));
        System.out.println(t.print());
        Table result = t.summarize("Bug编号", count).by("所属模块");
        System.out.println(result.print());
    }

    @Test
    void getSQLQuery() {
        pipeline.getSQLQuery();
    }
}