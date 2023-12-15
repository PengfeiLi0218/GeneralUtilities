package priv.lipengfei.utils;

import org.springframework.stereotype.Component;

@Component
public class ConfigLoader {
    private static boolean redisAvailable;

    public static boolean isRedisAvailable() {
        return redisAvailable;
    }

    public static void setRedisAvailable(boolean redisAvailable) {
        ConfigLoader.redisAvailable = redisAvailable;
    }




}
