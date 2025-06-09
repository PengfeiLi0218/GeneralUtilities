package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.sqlgenerator.sqlexpr.GroupCondition;
import priv.lipengfei.sqlgenerator.sqlexpr.SqlQuery;

import java.util.*;

/***
 * 用户会使用管道生成他们想要的操作流程
 * @author lipengfei
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Data
@Slf4j
public class Pipeline {
    private String id;
    private LinkedList<Cell> nodes = new LinkedList<>();

    @Override
    public String toString() {
        return String.join(" -> ", nodes.stream().map(c -> String.format("%s(%s)", c.getClass().getSimpleName(), c.getId())).toArray(String[]::new));
    }

    public Cell getFirstNode(){
        return this.nodes.peek();
    }

    public Cell getLastNode(){
        return this.nodes.peekLast();
    }

    public void generateId(){
        if(nodes.peek() == null || nodes.peekLast() == null){
            this.id = UUID.randomUUID().toString();
        }else {
            this.id = UUID.randomUUID().toString();// nodes.peek().getId() + nodes.peekLast().getId();
        }
    }
    public Pipeline(Cell node) {
        this.nodes = new LinkedList<>();
        this.nodes.add(node);
        generateId();
    }

    public Pipeline addNodes(Cell... node) {
        nodes.addAll(Arrays.asList(node));
        generateId();
        return this;
    }

    public Pipeline addNodes(List<Cell> node) {
        nodes.addAll(node);
        generateId();
        return this;
    }

    public boolean hasCell(String id) {
        return nodes.stream().anyMatch(n -> Objects.equals(n.getId(), id));
    }

    public Pipeline(Pipeline pipe){
        this.nodes = new LinkedList<>(pipe.getNodes());
        generateId();
    }

    public String getSQLQuery(){
        /**
         * 以Source和MergeItem为头
         * 以MergeItem和Sink为尾
         *
         * 1. 合并：
         * 多个相连的Selection合并成一个Selection：以最后的为准
         * 多个相连的Transformation合并成一个Transformation：连在一起就可以 List<Transformation>
         * 多个相连的Filter合并成一个Filter：以and相连
         *
         * 2. 生成SQL
         * 有selection了
         * transformation
         */
        SqlQuery sqlQuery = new SqlQuery();
        StringBuilder tmp = new StringBuilder();

        for (Cell item : this.nodes) {
            switch (item.getClass().getSimpleName()) {
                case "Source" -> {
                    sqlQuery.addTable(((Source) item).getTable());
                }
                case "Selection" -> {
                    if (Objects.equals(((Selection) item).getDistinctFlag(), "distinct")) {
                        sqlQuery.setDistinctFlag("distinct");
                    }
                    sqlQuery.addSelectExprs(((Selection) item).getCols());
                }
                case "Transformation" -> {
                    sqlQuery.addTransformExpr(((Transformation) item));
                }
                case "Filter" -> {
                    sqlQuery.addFilter(((Filter) item));
                }
                case "Aggregation" -> { // 聚合操作
                    // 如果select expr有操作，则生成一个临时表
                    // 重新建一个表
                    Aggregation aggregation = (Aggregation) item;
                    // 如果已经有selection在前，则创建新的临时表
                    if (!sqlQuery.getSelectExprs().isEmpty() || !sqlQuery.getTransformExprs().isEmpty()) {
                        String s = String.format("create temporary table previous_%s as %s\n", aggregation.getId(), sqlQuery);
                        log.info(s);
                        tmp.append(s);
                        sqlQuery = new SqlQuery();
                        sqlQuery.addTable("previous_"+aggregation.getId());
                    }
                    sqlQuery.addGroupCondition(new GroupCondition()
                            .extendCols(aggregation.getGrpCols()));
                    sqlQuery.addTransformExpr(aggregation.getAggCols().toArray(Transformation[]::new));
                    String s = String.format("create temporary table post_%s as %s\n", aggregation.getId(), sqlQuery);

                    sqlQuery.addTransformExpr(aggregation.getAggCols().toArray(Transformation[]::new));

                    tmp.append(s);
                    // 重建一个表
                    sqlQuery = new SqlQuery();
                    sqlQuery.addTable("post_"+aggregation.getId());
                }
                default -> log.error("not support cell type: {}", item.getClass().getSimpleName());
            }
            log.info("{} ===> {}", item, sqlQuery);
        }
        tmp.append(sqlQuery);

        log.info("SQL QUERY: \n{}", tmp);
        return tmp.toString();
    }
}
