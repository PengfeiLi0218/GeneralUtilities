package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/***
 * Group By
 * TODO:
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class GroupCondition {
    private List<String> cols = new ArrayList<>();
    private List<WhereCondition> havingConditions = new ArrayList<>();

    public GroupCondition setHavingConditions(List<WhereCondition> havingConditions) {
        this.havingConditions = havingConditions;
        return this;
    }

    public GroupCondition addHavingCondition(WhereCondition hc){
        if(this.havingConditions==null)
            this.havingConditions = new ArrayList<>();
        this.havingConditions.add(hc);
        return this;
    }

    public GroupCondition setCols(List<String> cols){
        this.cols = cols;
        return this;
    }

    public GroupCondition addCol(String col){
        this.cols.add(col);
        return this;
    }

    public GroupCondition extendCols(List<String> cols){
        this.cols.addAll(cols);
        return this;
    }

    private List<String> havingConditionToString(){
        List<String> ls = new ArrayList<>();
        if(this.havingConditions==null)
            this.havingConditions = new ArrayList<>();
        for (WhereCondition hc : this.havingConditions) {
            ls.add(hc.toString());
        }
        return ls;
    }

    @Override
    public String toString() {
        String s = String.format("GROUP BY %s", String.join(",", cols));

        if(havingConditions!=null && !havingConditions.isEmpty())
            s +=  String.format(" HAVING %s", String.join(",", this.havingConditionToString()));

        return s;
    }
}
