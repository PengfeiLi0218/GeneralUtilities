package priv.lipengfei.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RedisIDController {



    @GetMapping("/generateCode")
    public String count(){
        return "";
    }
}