package priv.lipengfei.sqlgenerator.pipeline;

import java.util.List;

/**
 * 多进一出
 */
public class MergeItem{
    private String tableA;
    private String tableB;
    private String joinType = "left";
    private List<Filter> conditions;

    public MergeItem setTableA(String table) {
        this.tableA = table;
        return this;
    }

    public MergeItem setTableB(String tableB) {
        this.tableB = tableB;
        return this;
    }

    public MergeItem setConditions(List<Filter> conditions) {
        this.conditions = conditions;
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
                tableA, joinType, tableB,
                String.join(" AND ",
                        conditions.stream().map(Filter::enumRoot).toList()
                ));
    }
}
