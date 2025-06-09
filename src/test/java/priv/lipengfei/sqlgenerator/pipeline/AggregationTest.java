package priv.lipengfei.sqlgenerator.pipeline;

import org.junit.jupiter.api.Test;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

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

        // 2. 单列分组 + 多个聚合函数（等效SQL: SELECT Region, COUNT(*), SUM(Revenue), AVG(Quantity) FROM sales GROUP BY Region）
        Table regionSummary = sales.summarize(
                "Region", AggregateFunctions.count
        ).by("Region");

        System.out.println("\n按地区汇总:");
        System.out.println(regionSummary.print());
    }
}