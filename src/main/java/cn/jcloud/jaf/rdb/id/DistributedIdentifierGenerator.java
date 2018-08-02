package cn.jcloud.jaf.rdb.id;

import cn.jcloud.jaf.common.handler.SpringContextHolder;
import cn.jcloud.jaf.common.util.IdUtils;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;

/**
 * 支持分布式
 * 规则：
 * Long类型，总计占53位(考虑JavaScript仅能表示53位整形)
 * 1.时间秒数，占 30 bit，可表示[34]年
 * 2.集群间计数器 占 {COUNTER_BITS} bit，可表示[1024]个数，这里使用redis来处理，每过{PER_TIME}毫秒进行清0
 * 3.实例内计数器 占 {SEQUENCE_BITS} bit，可表示[8096]个数,每过{COUNTER_EXPIRE_TIME}毫秒进行清0
 * 注意：这里的{COUNTER_EXPIRE_TIME}不宜设置过大，过大之后，当redis宕掉恢复后，如果计数又重新开始，且又
 * 在同一个{COUNTER_EXPIRE_TIME}时间窗口内，则会引起主键重复。同时又不宜设置过小，会导致频繁的读写redis。
 * 这里主要考虑的是一个{PER_TIME}时间窗口内，redis宕掉之后也无法恢复。
 * 实现参考：http://www.oschina.net/code/snippet_147955_25122
 * Created by Wei Han on 2016/2/2.
 *
 * @since 1.0
 */
public class DistributedIdentifierGenerator implements IdentifierGenerator, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedIdentifierGenerator.class);

    /*
     * 集群间计数过期时间，单位秒
     */
    private static final long COUNTER_EXPIRE_TIME = 60 * 5L;

    private RedisTemplate<String, String> redisTemplate;
    private RedisScript<String> script;

    private long lastTimestamp = -1L;

    private long sequence;

    private long counter;

    /*
     * 集群间计数器下次刷新时间
     */
    private long counterRefreshTimestamp = -1L;

    private String key;

    /*
     * 是否是分布式
     */
    private Boolean distributed;

    @Override
    public void configure(Type type, Properties params, Dialect d) {
        String jpaEntityName = params.getProperty(IdentifierGenerator.JPA_ENTITY_NAME);
        this.key = "idg:" + jpaEntityName;
    }

    @Override
    public Serializable generate(SessionImplementor session, Object object) {
        return nextId();
    }

    private long getCounter(long timestamp) {
        if (distributed == null) {
            synchronized (this) {
                if (distributed == null) {
                    distributed = SpringContextHolder.existsBean("idgRedisTemplate");
                }
            }
        }
        if (distributed) {
            return getDistributedCounter(timestamp);
        }
        return 0L;
    }

    /**
     * 获取集群间计数
     *
     * @return 集群间计数
     * @since 1.0
     */
    private long getDistributedCounter(long timestamp) {
        if (this.redisTemplate == null) {
            synchronized (this) {
                if (this.redisTemplate == null) {
                    this.redisTemplate = SpringContextHolder
                            .getBean("idgRedisTemplate", StringRedisTemplate.class);
                    this.script = (RedisScript<String>) SpringContextHolder.getBean("idgScript");
                }
            }
        }

        //通过脚本，确保key过期与新增在同一个原子操作中
        String resultStr = this.redisTemplate.execute(this.script, Collections.<String>emptyList(),
                key, String.valueOf(COUNTER_EXPIRE_TIME), String.valueOf(IdUtils.COUNTER_MASK),
                String.valueOf(IdUtils.COUNTER_BITS));
        String[] result = resultStr.split(",");
        counterRefreshTimestamp = timestamp + Long.parseLong(result[0]);
        return Long.parseLong(result[1]) & IdUtils.COUNTER_MASK;
    }

    private synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < this.lastTimestamp) {
            LOG.error(String.format("时间回退了. 拒绝直到%d的请求", this.lastTimestamp));
            throw new IllegalArgumentException(
                    String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds",
                            this.lastTimestamp - timestamp));
        }

        if (timestamp == this.lastTimestamp) {
            this.sequence = (this.sequence + 1) & IdUtils.SEQUENCE_MASK;
            if (this.sequence == 0) {
                timestamp = tilNextSecond(this.lastTimestamp);
            }
        } else {
            this.sequence = 0L;
        }
        if (timestamp > this.counterRefreshTimestamp) {
            this.counter = this.getCounter(timestamp);
        }

        this.lastTimestamp = timestamp;

        return IdUtils.generateId(timestamp, this.counter, this.sequence);
    }

    private static long tilNextSecond(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp == lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private static long timeGen() {
        return System.currentTimeMillis() / 1000;
    }
}
