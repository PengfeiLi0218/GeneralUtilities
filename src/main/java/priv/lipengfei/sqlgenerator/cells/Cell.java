package priv.lipengfei.sqlgenerator.cells;

import cn.hutool.core.util.IdUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tech.tablesaw.api.Table;

/**
 * @author lipengfei
 */
@Slf4j
@Data
public abstract class Cell {
    protected String shape = "";
    protected String id;

    public Cell() {
        this.id = IdUtil.simpleUUID();
    }

    public abstract Table execute(Table table);
}
