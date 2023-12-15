package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;

/**
 * Transformation：一进一出
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class Transformation extends Item {
    private String type="normal"; // 滑动窗口函数或者普通函数
    private String func; // 函数
    private List<String> args; // 函数参数
    private String out;
    private List<String> partitionCol;
    private List<String> orderCol;

    @Override
    public DataTable execute(DataTable table) {
        /*
          sum(col1) as col2 ======>  col2
          sum(col1)  ========>  sum(col1)
          col1  ========>  报错，不能没有函数
         */

        assert func!=null && !func.isEmpty();
        String tmp = String.format("%s(%s)", func, String.join(",", args));

        if(out!=null && !out.isEmpty())
            // 有别名用别名
            tmp = out;

        return new DataTable(this.name, table.getTableCols())
                .addTableCol(tmp);
    }

    @Override
    public String toString() {
        assert func!=null && !func.isEmpty();
        if(Objects.equals(type, "normal"))
            return String.format("%s(%s) as %s", func,
                    String.join(",", args),
                    out);
        else if(Objects.equals(type, "sliding"))
            return String.format("%s(%s) over (partition by %s order by %s)",
                    func, String.join(",", args),
                    String.join(",", partitionCol),
                    String.join(",", orderCol)
            );
        return null;
    }
}
