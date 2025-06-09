package priv.lipengfei.sqlgenerator.pipeline;

import priv.lipengfei.sqlgenerator.cells.Cell;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * 多进一出
 * @author lipengfei
 */
public class MergeItem extends Cell {
    private String joinType = "left"; // join类型，默认为left join
    private final List<Filter> conditions = new ArrayList<>();

    public MergeItem setConditions(List<Filter> conditions) {
        this.conditions.addAll(conditions);
        return this;
    }

    public MergeItem setConditions(Filter... conditions) {
        this.conditions.addAll(List.of(conditions));
        return this;
    }

    public MergeItem setJoinType(String joinType) {
        this.joinType = joinType;
        return this;
    }

    public DataTable execute(DataTable tableA, DataTable tableB) {
        assert tableA != null;
        assert tableB != null;
        return new DataTable();
    }

    @Override
    public String toString() {
        return String.format("(%s) a %s join (%s) b on %s",
                "mainTable", joinType, "branchTable",
                String.join(" AND ", new ArrayList<>()
//                        conditions.stream().map(Filter::enumRoot).toList()
                ));
    }

    @Override
    public Table execute(Table table) {
        return table;
    }
}
