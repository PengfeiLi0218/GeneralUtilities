package priv.lipengfei.service;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

@Service
public class CodeGeneratorFactory {
    private final StringRedisTemplate redisTemplate;

    private final Random random = new Random();
    @Value("${autoincrement.key:cnt}")
    private String aino;

    public CodeGeneratorFactory(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    private Long getAutoincrementCode(){
        try {
            System.out.printf("aino: %s", aino);
            return redisTemplate.opsForValue().increment(aino);
        }catch (Exception ex){
            return 0L;
        }
    }

    public String getRandom(int length){
        return String.format("%0"+length+"d", random.nextInt((int)Math.pow(10,length)));
    }

    public String getAutoIncrement(int length){
        Long id = getAutoincrementCode();
        System.out.println(id);
        return String.format("%0"+length+"d", id);
    }

    public boolean isDateValid(String date){
        String l = String.format("%-14d", Long.parseLong(date)).replace(' ', '1');

        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        try{
            String format2 = sdf.format(sdf.parse(l));
            return format2.equals(l);
        }catch (Exception ex){
            return false;
        }
    }
    public CodeGenerator getCodeGenerator(String path) throws IOException {
        Gson gson = new Gson();
        File file = new File(path);
        String content = FileUtils.readFileToString(file, "UTF-8");
        return gson.fromJson(content, CodeGenerator.class);
    }

    public String generate(CodeGenerator cg, Map<String, String> input){
        StringBuilder sb = new StringBuilder(cg.getLength());
        String out = "";
        for (Rule rule : cg.getRules()) {
            String mapper = rule.getMapper();
            int length = rule.getEnd()-rule.getStart()+1;

            if(Objects.equals(mapper, "datetime")){
                String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
                out = timeStamp.substring(0, length);
            }else if(Objects.equals(mapper, "random")){
                out = this.getRandom(length);
            }else if(Objects.equals(mapper, "autoincrement")){
                out = this.getAutoIncrement(length);
            }else if(cg.getMappers().containsKey(mapper)){
                Map<String, String> mapperObject = cg.getMappers().get(mapper);
                if(Objects.equals(mapperObject.get("type"), "enum")) {
                    String inputParam = input.get(mapper);
                    out = mapperObject.get(inputParam);
                }else if(Objects.equals(mapperObject.get("type"), "substr")){
                    String inputParam = input.get(mapper);
                    out = inputParam.substring(Integer.parseInt(mapperObject.get("start")), Integer.parseInt(mapperObject.get("end")));
                }
            }else{
                out = mapper;
            }
            sb.replace(rule.getStart(), rule.getEnd(), out);
        }
        return String.valueOf(sb);
    }

    public boolean check(CodeGenerator cg, String code, Map<String, String> input){
        if(cg.getLength() != code.length()){
            System.out.println("字符串长度不符!!!");
            return false;
        }
        for (Rule rule : cg.getRules()) {
            String substring = code.substring(rule.getStart(), rule.getEnd()+1);
            String mapper = rule.getMapper();
            if(cg.getMappers().containsKey(mapper)){
                Map<String, String> mapperObject = cg.getMappers().get(mapper);

                String inputParam = input.get(mapper);
                if(Objects.equals(mapperObject.get("type"), "enum")) {
                    if(!Objects.equals(mapperObject.get(inputParam), substring)){
                        System.out.printf("映射关系的，%s 和 %s 不相同!!!%n", substring, mapperObject.get(inputParam));
                        return false;
                    }
                }else if(Objects.equals(mapperObject.get("type"), "substr")){
                    String s1 = inputParam.substring(Integer.parseInt(mapperObject.get("start")), Integer.parseInt(mapperObject.get("end")));
                    if(!s1.equals(substring)){
                        System.out.printf("字符串截取的，%s 和 %s 不相同!!!%n", s1, substring);
                        return false;
                    }
                }

            }else if(Objects.equals(mapper, "datetime")){
                if(!this.isDateValid(substring)){
                    System.out.println("时间不匹配!!!");
                    return false;
                }
            }else if(!Objects.equals(mapper, "random") && !Objects.equals(mapper, "autoincrement")){
                if(!substring.equals(mapper)){
                    System.out.printf("字符串本身的，%s 和 %s 不相同!!!", substring, mapper);
                    return false;
                }
            }
        }
        return true;
    }
}
