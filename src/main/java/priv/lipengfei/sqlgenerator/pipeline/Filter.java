package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.basic.TreeNode;
import priv.lipengfei.sqlgenerator.sqlexpr.LogisticRelation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import priv.lipengfei.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 继承至Item
 * 选择某些行
 * 行形式的截取
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Filter extends Item {
    private List<WhereCondition> filterConditions = new ArrayList<>();
    private LogisticRelation relationship; // or或者and Condition

    public Filter setFilterConditions(List<WhereCondition> filterConditions) {
        this.filterConditions = new ArrayList<>(filterConditions);
        return this;
    }

    public Filter addFilterCondition(WhereCondition filterConditions) {
        if(this.filterConditions==null)
            this.filterConditions = new ArrayList<>();
        this.filterConditions.add(filterConditions);
        return this;
    }

    @Override
    public DataTable execute(DataTable table) {
        return new DataTable(table.getTableName())
                .setTableCols(table.getTableCols());
    }

    @Override
    public String toString() {
        return String.format("select * from %s where %s", name, enumRoot());
    }

    // 遍历该node的各个节点
    private String enumerate(TreeNode node){
        String tmp = null;
        if(node.getClass()==LogisticRelation.class) {
            List<String> strings = node.getChildNodes().stream().map(this::enumerate).toList();
            tmp = " ("+String.join(" "+((LogisticRelation)node).getRole().toUpperCase(Locale.ROOT)+" ", strings)+") ";
        }else if(node.getClass()==WhereCondition.class){
            tmp = node.toString();
        }
        return tmp;
    }

    // 遍历relationship
    public String enumRoot(){
        // 如果relationship为空，默认filterCondition列表用and链接
        if (this.relationship == null){
            return String.join(" AND ", Utils.objsToString(filterConditions));
        }else {
            return enumerate(this.relationship).replaceAll(" +", " ");
        }
    }
}
