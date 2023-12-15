package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import priv.lipengfei.sqlgenerator.sqlexpr.GroupCondition;
import priv.lipengfei.sqlgenerator.sqlexpr.SQLQuery;
import priv.lipengfei.utils.JSONParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/***
 * 用户会使用管道生成他们想要的操作流程
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Pipeline {
    private List<Item> items = new ArrayList<>();

    public Pipeline addItem(Item item){
        if(items == null)
            items = new ArrayList<>();
        items.add(item);
        return this;
    }

    public String getSQLQuery(){
        SQLQuery sqlQuery = new SQLQuery();
        StringBuilder tmp = new StringBuilder();
        int cnt = 0;
        for (Item item : items) {
            switch (item.getClass().getSimpleName()) {
                case "Source" -> sqlQuery.addTable(((Source) item).getTable());
                case "Selection" -> {
                    if (Objects.equals(((Selection) item).getDistinctFlag(), "distinct"))
                        sqlQuery.setDistinctFlag("distinct");
                    sqlQuery.extendSelectExprs(((Selection) item).getCols());
                }
                case "Filter" -> sqlQuery.addFilter(((Filter) item));
                case "Aggregation" -> {
                    // 聚合操作
                    // 如果select expr有操作，则生成一个临时表
                    // 重新建一个表
                    if (sqlQuery.getSelectExprs() != null && sqlQuery.getSelectExprs().size() > 0) {
                        String tablename = "ttable" + cnt;
                        String s = String.format("create temporary table %s as %s\n", tablename, sqlQuery);
//                        System.out.print(s);
                        tmp.append(s);
                        sqlQuery = new SQLQuery();
                        sqlQuery.addTable(tablename);
                        cnt++;
                    }
                    sqlQuery.setGroupCondition(new GroupCondition()
                            .extendCols(((Aggregation) item).getGrpCols()));
                    sqlQuery.extendSelectExprsString(((Aggregation) item).getGrpCols());
                    sqlQuery.extendSelectExprs(((Aggregation) item).getAggCols());
                    String tablename = "ttable" + cnt;
                    String s = String.format("create temporary table %s as %s\n", tablename, sqlQuery);
//                    System.out.print(s);
                    tmp.append(s);
//                System.out.println(sqlQuery);
                    // 重建一个表
                    sqlQuery = new SQLQuery();
                    sqlQuery.addTable(tablename);
                }
            }
        }
//        System.out.println(sqlQuery);
        tmp.append(sqlQuery);
        return tmp.toString();
    }

    public void execute(){
        DataTable dataTable = null;
        for (Item item : items) {
            dataTable = item.execute(dataTable);
            System.out.println(item.name + " : " + dataTable);
        }
    }

    public String toJson(){
//        StringBuilder s = new StringBuilder();
        List<Map<String, Object>> maps = new ArrayList<>();
        for (Item item : this.items) {
            Map<String, Object> stringObjectMap = JSONParser.toMapByReflect(item);
            stringObjectMap.put("class", item.getClass().getName());
            maps.add(stringObjectMap);
//            s.append(JSONParser.toJsonString(stringObjectMap)).append("\n");
        }

        return JSONParser.toJsonString(maps);
    }

    public static Pipeline fromJson(String s){
        List map = JSONParser.fromJsonString(s, List.class);
        System.out.println(map);
        return new Pipeline();
    }
}
