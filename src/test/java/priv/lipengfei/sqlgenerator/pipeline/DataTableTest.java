package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.cells.Cells;
import priv.lipengfei.sqlgenerator.cells.Edge;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import priv.lipengfei.utils.CellsJsonParser;

class DataTableTest {
    @Test
    void GroupTest(){
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
//                                "F-CSG-MK0513-02",
//                                "F-CSG-MK0513-01",
//                                "F-CSG-MK0512-01"),
                        new WhereCondition("rn", "=", "1")
                        );


        Edge edge1 = new Edge().setTargetId(table.getId()).setSourceId(transformer1.getId());
        Edge edge2 = new Edge().setTargetId(transformer1.getId()).setSourceId(filter.getId());


        Cells cells = new Cells().setCells(table, transformer1, filter, edge1, edge2);

        System.out.println(CellsJsonParser.toJsonString(cells));
    }
}