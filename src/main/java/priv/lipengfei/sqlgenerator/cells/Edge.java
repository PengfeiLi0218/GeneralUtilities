package priv.lipengfei.sqlgenerator.cells;

import lombok.Data;
import tech.tablesaw.api.Table;

/**
 * @author lipengfei
 */
@Data
public class Edge extends Cell{
    private String sourceId;
    private String targetId;

    public Edge() {
        this.shape = "edge";
    }

    @Override
    public Table execute(Table table) {
        return null;
    }

    public Edge(String sourceId, String targetId) {
        this.shape = "edge";
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    public Edge setSourceId(String sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    public Edge setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", sourceId, targetId);
    }
}
