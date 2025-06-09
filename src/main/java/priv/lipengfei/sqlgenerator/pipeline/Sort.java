package priv.lipengfei.sqlgenerator.pipeline;

import lombok.AllArgsConstructor;
import lombok.Data;
import priv.lipengfei.sqlgenerator.cells.Cell;
import tech.tablesaw.api.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lipengfei
 */
@Data
@AllArgsConstructor
public class Sort {
    /*
     true为acs，false为desc
     */

    String orderby;
    Boolean asc = true;

    public Sort(String orderby){
        this.orderby = orderby;
    }

    public Sort setOrderby(String orderby) {
        this.orderby = orderby;
        return this;
    }

    public Sort setAsc(boolean asc) {
        this.asc = asc;
        return this;
    }
}
