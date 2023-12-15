package priv.lipengfei.service;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Component;
import priv.lipengfei.utils.ConfigLoader;

@Component
public class RedisHealthIndicator implements HealthIndicator {
    private final RedisConnectionFactory rcf;

    public RedisHealthIndicator(RedisConnectionFactory rcf) {
        this.rcf = rcf;
    }

    @Override
    public Health health() {
        try {
            this.rcf.getConnection().close();
            ConfigLoader.setRedisAvailable(true);
            return Health.up().build();
        } catch (Exception ex) {
            ConfigLoader.setRedisAvailable(false);
            return Health.down(ex).build();
        }
    }

}
