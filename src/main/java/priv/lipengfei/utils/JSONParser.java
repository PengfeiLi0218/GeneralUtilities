package priv.lipengfei.utils;

import com.google.gson.*;
import priv.lipengfei.basic.TreeNode;
import priv.lipengfei.sqlgenerator.pipeline.*;
import priv.lipengfei.sqlgenerator.sqlexpr.LogisticRelation;
import priv.lipengfei.sqlgenerator.sqlexpr.WhereCondition;
import priv.lipengfei.sqlgenerator.sqlexpr.*;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JSONParser {
    // 抽象类要制定哪个子类
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Item.class,
            (JsonDeserializer<Item>) (json, typeOfT, context) -> {
                JsonObject object = json.getAsJsonObject();

                String type = object.get("className").getAsString();

                if ("Source".equals(type)) {
                    return context.deserialize(json, Source.class);
                } else if ("Selection".equals(type)) {
                    return context.deserialize(json, Selection.class);
                } else if ("Aggregation".equals(type)) {
                    return context.deserialize(json, Aggregation.class);
                } else if ("Filter".equals(type)) {
                    return context.deserialize(json, Filter.class);
                } else if ("Transformation".equals(type)) {
                    return context.deserialize(json, Transformation.class);
                } else {
                    return null;
                }
            }).registerTypeAdapter(TreeNode.class,
            (JsonDeserializer<TreeNode>) (json, typeOfT, context) -> {
                JsonObject object = json.getAsJsonObject();

                String type = object.get("className").getAsString();

                if ("WhereCondition".equals(type)) {
                    return context.deserialize(json, WhereCondition.class);
                } else if ("LogisticRelation".equals(type)) {
                    return context.deserialize(json, LogisticRelation.class);
                } else {
                    return null;
                }
            }).create();

    public static String toJsonString(Object o){
        return gson.toJson(o);
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
