package priv.lipengfei.sqlgenerator.cells;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.pipeline.Expression;
import priv.lipengfei.sqlgenerator.pipeline.Filter;
import priv.lipengfei.sqlgenerator.pipeline.Source;
import priv.lipengfei.sqlgenerator.pipeline.Transformation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import priv.lipengfei.utils.CellsJsonParser;

import static org.junit.jupiter.api.Assertions.*;

class CellsTest {
    static Cells cells = new Cells();

    @BeforeAll
    static void setUp() {
        Source table = new Source("业扩工单");
        Transformation transformer1 = new Transformation()
                .setPartitionBy("wo_id")
                .addOrderBy("task_num", false)
                .setExpression(new Expression().setFunc("row_number").setAlias("rn"));

        Filter filter = new Filter().
                setFilterConditions(
//                        new WhereCondition().setVar("wo_status_code").setOp("in").setVal("3", "4"),
//                        new WhereCondition("operated_time", ">=", "{now_date - 1month}"),
//                        new WhereCondition("operated_time", "<=", "{now_date}"),
//                        new WhereCondition().setVar("busi_type_no").setOp("in")
//                                .setVal("F-CSG-MK0513-03",
//                                        "F-CSG-MK0513-02",
//                                        "F-CSG-MK0513-01",
//                                        "F-CSG-MK0512-01"),
//                        new WhereCondition("rn", "=", "1")
                );


        Edge edge1 = new Edge().setSourceId(table.getId()).setTargetId(transformer1.getId());
        Edge edge2 = new Edge().setSourceId(transformer1.getId()).setTargetId(filter.getId());


        cells.setCells(table, transformer1, filter, edge1, edge2);

        System.out.println(CellsJsonParser.toJsonString(cells));
    }

    @Test
    void hasPrevious() {

        System.out.println(cells.hasPrevious(cells.getCells().get(0).id));

        System.out.println(cells.getPrevious(cells.getCells().get(0).id));
    }

    @Test
    void getPipeline() {
    }

    @Test
    void getPipelines() {
    }
}