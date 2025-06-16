package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.Test;
import tech.tablesaw.aggregate.AggregateFunction;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.HashMap;
import java.util.Map;

class AggregationTest {

    @Test
    void execute() {
        // 1. 创建示例数据表
        Table sales = Table.create("Sales")
                .addColumns(
                        StringColumn.create("Region", "East", "East", "West", "West", "West"),
                        StringColumn.create("Product", "A", "A", "B", "B", "C"),
                        DoubleColumn.create("Revenue", 100.0, 150.0, 200.0, 250.0, 300.0),
                        IntColumn.create("Quantity", 10, 15, 20, 25, 30)
                );

        System.out.println("原始数据:");
        System.out.println(sales.print());

        Aggregation aggregation = new Aggregation()
                .addGrpCols("Region")
                .addAggFuncs(
                        new Transformation().setExpression(new Expression("Region").setFunc("count")).setType("aggregation"),
                        new Transformation().setExpression(new Expression("Revenue").setFunc("sum")).setType("aggregation"),
                        new Transformation().setExpression(new Expression("Quantity").setFunc("mean")).setType("aggregation")
                );


        Table summary = aggregation.execute(sales);
        System.out.println("\n按地区汇总:");
        System.out.println(summary.print());

        // 2. 单列分组 + 多个聚合函数（等效SQL: SELECT Region, COUNT(*), SUM(Revenue), AVG(Quantity) FROM sales GROUP BY Region）
//        Table regionSummary = sales.summarize(
//                "Region", "Revenue", "Quantity",
//                AggregateFunctions.count, AggregateFunctions.sum, AggregateFunctions.mean
//        ).by("Region");
//
//        System.out.println("\n按地区汇总:");
//        System.out.println(regionSummary.print());
    }
}