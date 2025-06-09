package priv.lipengfei.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import priv.lipengfei.basic.TreeNode;
import priv.lipengfei.sqlgenerator.cells.*;
import priv.lipengfei.sqlgenerator.pipeline.*;
import priv.lipengfei.sqlgenerator.sqlexpr.LogisticRelation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lipengfei
 */
public class CellsJsonParser {
    // 抽象类要制定哪个子类
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Cell.class,
            (JsonDeserializer<Cell>) (json, typeOfT, context) -> {
                JsonObject object = json.getAsJsonObject();

                String type = object.get("shape").getAsString();

                if (type.startsWith("condition-box")) {
                    return context.deserialize(json, Transformation.class);
                } else if ("database-frame".equals(type)) {
                    return context.deserialize(json, Source.class);
                } else if ("edge".equals(type)) {
                    return context.deserialize(json, Edge.class);
                } else {
                    return null;
                }
            }).create();

    public static String toJsonString(Cells cells){
        cells.getCells().forEach(cell -> {
            System.out.println(cell.getClass());
        });
        return gson.toJson(cells);
    }

    public static <T> T fromJsonString(String jsonInString, Class<T> tClass){
        return gson.fromJson(jsonInString, tClass);
    }

    public static  <T> Map<String, Object> toMapByReflect(T obj) {
        Field[] fields = obj.getClass().getDeclaredFields();
        Map<String, Object> map = new HashMap<>();

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (Field f : fields) {
                f.setAccessible(true);
                Object val = f.get(obj);
                if (f.getType() == Date.class) {
                    map.put(f.getName(), sdf.format(val));
                } else {
                    map.put(f.getName(), val);
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }

        return map;
    }
}
