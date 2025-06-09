package priv.lipengfei.sqlgenerator.pipeline;

import lombok.Data;
import priv.lipengfei.basic.TreeNode;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.sqlgenerator.sqlexpr.LogisticRelation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import priv.lipengfei.utils.Utils;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 继承至Item
 * 选择某些行
 * 行形式的截取
 * @author lipengfei
 */
@Data
public class Filter extends Cell {
    private LogisticRelation relationship = new LogisticRelation("and"); // or或者and Condition

    public Filter setRelationship(LogisticRelation relationship) {
        this.relationship = relationship;
        return this;
    }

    public Filter() {
        this.shape = "filter";
    }

    public Filter(WhereCondition... filterConditions) {
        this.setRelationship(new LogisticRelation("or")
                .setChildNodes(List.of(filterConditions)));
    }

    public Filter setFilterConditions(WhereCondition... filterConditions) {
        this.relationship = new LogisticRelation("or").setChildNodes(List.of(filterConditions));
        return this;
    }

    @Override
    public String toString() {
        return String.format("Filter(%s)", this.id);
    }

    /**
     * 遍历该node的各个节点
      */
    private String enumerate(TreeNode node) throws Exception {
        String tmp = null;
        if(node instanceof LogisticRelation) {
            List<String> strings = node.getChildNodes().stream().map(c -> {
                try {
                    return this.enumerate(c);
                } catch (Exception e) {
                    e.printStackTrace();
                    return "";
                }
            }).toList();
            tmp = " ("+String.join(" "+((LogisticRelation)node).getRole().toUpperCase(Locale.ROOT)+" ", strings)+") ";
        }else if(node instanceof WhereCondition){
            tmp = ((WhereCondition) node).generateSql();
        }
        return tmp;
    }

    // 遍历relationship
    public String enumRoot() throws Exception {
        // 如果relationship为空，默认filterCondition列表用and链接
        return enumerate(this.relationship).replaceAll(" +", " ");
    }

    @Override
    public Table execute(Table table) {
        try {
            return table.where(this.relationship.toSelection(table));
        } catch (Exception e) {
            e.printStackTrace();
            return table;
        }
    }

    public String generateSql() throws Exception {
        return enumRoot();
    }
}
