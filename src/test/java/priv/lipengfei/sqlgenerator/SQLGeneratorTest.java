package priv.lipengfei.sqlgenerator;

import org.junit.jupiter.api.Test;
import priv.lipengfei.sqlgenerator.pipeline.*;
import priv.lipengfei.sqlgenerator.pipeline.Pipeline;
import priv.lipengfei.sqlgenerator.sqlexpr.*;
import priv.lipengfei.utils.JSONParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLGeneratorTest {
    @Test
    void selectExpressionTest(){
        // count(sss) as cnt
        System.out.println(new SelectExpression("count", "sss", "cnt"));
        // count(sss)
        System.out.println(new SelectExpression("count", "sss"));
        // sss as sss_alias
        System.out.println(new SelectExpression("sss").setAlias("sss_alias"));
    }

    @Test
    void GroupCondictionTest(){
        // group by col1, col2
        // List<String> cols = new ArrayList<>();
        // cols.add("col1");
        // cols.add("col2");
        List <String> cols = Arrays.asList("col1", "col2");
        System.out.println(new GroupCondition().setCols(cols));

        // group by col1
        List <String> cols2 = List.of("col1");
        System.out.println(new GroupCondition().setCols(cols2));

        // group by col1,col2 having sum(col2)>100
        WhereCondition hc = new WhereCondition("sum", "col2", ">", "100");

        System.out.println(new GroupCondition().setCols(cols).addHavingCondition(hc));
    }

    @Test
    void SQLQueryTest() throws IOException {
        List<SelectExpression> exprs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            exprs.add(new SelectExpression("col"+i));
        }

        List<String> tbls = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            tbls.add("table"+i);
        }

        List<WhereCondition> wcs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            wcs.add(new WhereCondition("col"+i, ">", "123"));
        }

        GroupCondition gc = new GroupCondition()
                .setCols(Arrays.asList("col1", "col2", "col3"));
//                .addHavingCondition();

        SQLQuery sq = new SQLQuery()
                .setDistinctFlag("")
                .setSelectExprs(exprs)
                .setTableReference(tbls)
                .setLimitNo(5)
                .addFilter(new Filter().setFilterConditions(wcs))
                .setGroupCondition(gc);

        String jsonInString = JSONParser.toJsonString(sq);

        System.out.println(sq);
        System.out.println(jsonInString);

        SQLQuery sq2 = JSONParser.fromJsonString(jsonInString, SQLQuery.class);

        System.out.println(sq2);

//        String fileName = "/Users/lipengfei/Code/general-utilities/src/main/resources/SQLQuery.json";
//        Path path = new File(fileName).toPath();
//        Reader reader = Files.newBufferedReader(path,
//                StandardCharsets.UTF_8);
//        SQLQuery sq3 = JSONParser.fromJsonString(
//                reader,
//                SQLQuery.class);
//        System.out.println(sq3);
    }

    Filter generate(){
        WhereCondition col1 = new WhereCondition("col1", ">", "100");
        WhereCondition col2 = new WhereCondition("col2", ">=", "101");
        WhereCondition col3 = new WhereCondition("col3", "<", "102");
        WhereCondition col4 = new WhereCondition("col4", "==", "103");
        WhereCondition col5 = new WhereCondition("col5", "<=", "104");
        WhereCondition col6 = new WhereCondition("col6", "!=", "105");
        WhereCondition col7 = new WhereCondition("col7", "<>", "106");
        WhereCondition col8 = new WhereCondition("col8", "==", "107");
        WhereCondition col9 = new WhereCondition("col9", "!=", "108");

        LogisticRelation r1 = new LogisticRelation("and");
        r1.setChildNodes(Arrays.asList(col1, col2, col3));

        LogisticRelation r2 = new LogisticRelation("and");
        r2.setChildNodes(Arrays.asList(col4, col5));

        LogisticRelation r3 = new LogisticRelation("or");
        r3.setChildNodes(Arrays.asList(col7, col8));

        LogisticRelation r4 = new LogisticRelation("and");
        r4.setChildNodes(Arrays.asList(col6, col9));

        LogisticRelation r5 = new LogisticRelation("or");
        r5.setChildNodes(Arrays.asList(r1, r2));

        LogisticRelation r6 = new LogisticRelation("or");
        r6.setChildNodes(Arrays.asList(r3, r4));

        LogisticRelation r7 = new LogisticRelation("and");
        r7.setChildNodes(Arrays.asList(r5, r6));

        Filter filter = new Filter(Arrays.asList(col1, col2, col3, col4, col5, col6, col7, col8, col9),
                r7);
        return filter;
    }

    @Test
    void JSONParserTest(){
        // 用Gson直接转json不可以
        // 会报错com.google.gson.JsonIOException: Abstract classes can't be instantiated! Register an InstanceCreator or a TypeAdapter for this type.
        // 因为Gson不知道转为哪个子类
        // 需要给pipeline或者有抽象类特定写toJson函数和fromJson函数
        Pipeline pipeline = new Pipeline()
                .addItem(new Source("table1")
                        .setTableCols(Arrays.asList("col1", "col2", "col3", "col4", "col5")))
                .addItem(new Selection()
                        .setDistinctFlag("distinct")
                        .addSelectExpression(new SelectExpression("col1"))
                        .addSelectExpression(new SelectExpression("col2"))
                        .addSelectExpression(new SelectExpression("col3")))
                .addItem(new Filter()
                        .addFilterCondition(new WhereCondition("col1", ">", "100"))
                        .addFilterCondition(new WhereCondition("col3", "==", "4"))
                )
                .addItem(new Filter()
                        .addFilterCondition(new WhereCondition("col2", "<", "10")))
                .addItem(generate())
                .addItem(new Aggregation()
                        .addGrpCol("col1").addGrpCol("col2")
                        .addAggFunc(new SelectExpression("sum", "col3", "sum_col3"))
                        .addAggFunc(new SelectExpression("avg", "col3", "avg_col3"))
                ).addItem(new Filter()
                        .addFilterCondition(new WhereCondition("col1", "<", "200")));

//        String s = pipeline.toJson();
//        System.out.println(s);

        System.out.println(pipeline.getSQLQuery());

        String s = JSONParser.toJsonString(pipeline);

        System.out.println(s);

//        System.out.println(JSONParser.toMapByReflect(pipeline));
        Pipeline p = JSONParser.fromJsonString(s, Pipeline.class);
        System.out.println(p.getSQLQuery());
    }
}
