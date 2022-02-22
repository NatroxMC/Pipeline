package de.notion.pipeline.redis;

import de.notion.pipeline.config.PartConfig;
import de.notion.pipeline.config.part.DataUpdaterConfig;
import de.notion.pipeline.config.part.GlobalCacheConfig;
import de.notion.pipeline.part.cache.GlobalCache;
import de.notion.pipeline.part.local.LocalCache;
import de.notion.pipeline.part.local.updater.DataUpdaterService;
import de.notion.pipeline.redis.cache.RedisCache;
import de.notion.pipeline.redis.updater.RedisDataUpdaterService;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.concurrent.TimeUnit;

public class RedisConfig implements DataUpdaterConfig, GlobalCacheConfig, PartConfig {

    private final boolean clusterMode;
    private final String[] addresses;
    private final String password;

    private RedissonClient redissonClient;
    private boolean connected;

    public RedisConfig(boolean useCluster, String password, String... addresses) {
        this.clusterMode = useCluster;
        this.addresses = addresses;
        this.password = password;
        this.connected = false;
    }

    @Override
    public void load() {
        if (isLoaded()) return;
        if (addresses.length == 0)
            throw new IllegalArgumentException("Address Array empty");
        var config = new Config();
        if (clusterMode) {
            var clusterServersConfig = config.useClusterServers();
            clusterServersConfig.addNodeAddress(addresses);

            if (!password.isEmpty())
                clusterServersConfig.addNodeAddress(addresses).setPassword(password);
            else
                clusterServersConfig.addNodeAddress(addresses);
        } else {
            var address = addresses[0];
            SingleServerConfig singleServerConfig = config.useSingleServer();
            singleServerConfig.setSubscriptionsPerConnection(30);

            if (!password.isEmpty())
                singleServerConfig.setAddress(address).setPassword(password);
            else
                singleServerConfig.setAddress(address);
        }
        config.setNettyThreads(4);
        config.setThreads(4);
        this.redissonClient = Redisson.create(config);
        connected = true;
    }

    @Override
    public void shutdown() {
        redissonClient.shutdown(0, 2, TimeUnit.SECONDS);
        connected = false;
    }

    @Override
    public boolean isLoaded() {
        return connected;
    }

    @Override
    public DataUpdaterService constructDataManipulator(LocalCache localCache) {
        return new RedisDataUpdaterService(redissonClient, localCache);
    }

    @Override
    public GlobalCache constructGlobalCache() {
        return new RedisCache(redissonClient);
    }
}