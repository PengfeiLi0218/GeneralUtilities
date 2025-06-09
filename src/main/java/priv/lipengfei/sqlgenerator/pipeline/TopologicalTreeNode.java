package priv.lipengfei.sqlgenerator.pipeline;

import lombok.Data;
import priv.lipengfei.sqlgenerator.cells.Cell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lipengfei
 */
@Data
public class TopologicalTreeNode {
    private Cell node;
    private List<String> nextNodes;
    private List<String> parentNodes;

    @Override
    public String toString() {
        return String.format("树节点[%s](%s)：\n{%s\n子节点: %s\n父节点：%s\n}", node.getClass().getSimpleName(), node.getId(), node, nextNodes, parentNodes);
    }

    public long getInDegree() {
        return parentNodes.size();
    }

    public long getOutDegree() {
        return nextNodes.size();
    }

    public TopologicalTreeNode(Cell node) {
        this.node = node;
        this.nextNodes = new ArrayList<>();
        this.parentNodes = new ArrayList<>();
    }

    public TopologicalTreeNode() {
        this.node = null;
        this.nextNodes = new ArrayList<>();
        this.parentNodes = new ArrayList<>();
    }

    public TopologicalTreeNode addNextNode(String... nextNode) {
        this.nextNodes.addAll(Arrays.asList(nextNode));
        return this;
    }

    public TopologicalTreeNode addParentNode(String... parentNode) {
        this.parentNodes.addAll(Arrays.asList(parentNode));
        return this;
    }


    public TopologicalTreeNode setNode(Cell node) {
        this.node = node;
        return this;
    }

    public TopologicalTreeNode setNextNodes(List<String> nextNodes) {
        this.nextNodes = nextNodes;
        return this;
    }

    public boolean chechAvailable() {
        // TODO
        // 除了MergeItem
        return true;
    }

}
