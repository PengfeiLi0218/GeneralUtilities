package priv.lipengfei.generalutilities;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.core.StringRedisTemplate;
import priv.lipengfei.service.CodeGenerator;
import priv.lipengfei.service.CodeGeneratorFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
@ComponentScan
class GeneralUtilitiesApplicationTests {
    @Autowired
    StringRedisTemplate srt;

    @Test
    void contextLoads() {
        try {
            CodeGeneratorFactory cgf = new CodeGeneratorFactory(srt);

//            Long result = srt.opsForValue().increment("cnt");
//            System.out.println(result);

            String path = "/Users/lipengfei/Code/general-utilities/src/main/resources/codeGeneratorRuler.json";

            CodeGenerator cg = cgf.getCodeGenerator(path);
            System.out.println(cg.getLength());
            System.out.println(cg.getRules());
            System.out.println(cg.getMappers());

            Map<String, String> m = new HashMap<>();
            m.put("p1", "A");
            m.put("p2", "12342513");
            String code = cgf.generate(cg, m);
            System.out.println(code);

            System.out.println(cgf.check(cg, code, m));
            System.out.println(cgf.check(cg, "ATT202311100200142", m));
            System.out.println(cgf.check(cg, "ATT202311100200145", m));
            System.out.println(cgf.check(cg, "ATT202311100200742", m));
            System.out.println(cgf.check(cg, "SSS202311100200342", m));
            System.out.println(cgf.check(cg, "ATT202321100200342", m));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void redisTest(){
        String result = srt.opsForValue().get("cnt");
        System.out.println(result);
    }

    @Test
    void checkDateTest(){
        CodeGeneratorFactory cgf = new CodeGeneratorFactory(srt);
        try {
            System.out.println(cgf.isDateValid("20221102"));
            System.out.println(cgf.isDateValid("202211"));
            System.out.println(cgf.isDateValid("2022"));
            System.out.println(cgf.isDateValid("20221"));
            System.out.println(cgf.isDateValid("2022110"));
            System.out.println(cgf.isDateValid("202211021"));
            System.out.println(cgf.isDateValid("202221021"));
            System.out.println(cgf.isDateValid("202213021"));
            System.out.println(cgf.isDateValid("202211001"));
            System.out.println(cgf.isDateValid("202211026"));
            System.out.println(cgf.isDateValid("202211341"));
            System.out.println(cgf.isDateValid("2022110225"));
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
