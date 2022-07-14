/*
 * Copyright 2020-2022 NatroxMC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.natrox.pipeline.demo;

import de.natrox.common.util.UuidUtil;
import de.natrox.pipeline.caffeine.CaffeineProvider;
import de.natrox.pipeline.demo.onlinetime.ObjectOnlineTimeManager;
import de.natrox.pipeline.demo.onlinetime.OnlineTimeManager;
import de.natrox.pipeline.mongo.MongoConfig;
import de.natrox.pipeline.mongo.MongoProvider;
import de.natrox.pipeline.redis.RedisConfig;
import de.natrox.pipeline.redis.RedisProvider;
import de.natrox.pipeline.repository.Pipeline;

import java.util.UUID;

public final class GlobalDemoMain {

    public static void main(String[] args) throws InterruptedException {
        MongoConfig mongoConfig = MongoConfig
            .create()
            .setHost("127.0.0.1")
            .setPort(27017)
            .setDatabase("demo");
        MongoProvider mongoProvider = MongoProvider.of(mongoConfig);

        RedisConfig redisConfig = RedisConfig
            .create()
            .addEndpoint(endpoint -> endpoint.setHost("127.0.0.1").setPort(6379).setDatabase(0));
        RedisProvider redisProvider = RedisProvider.of(redisConfig);

        CaffeineProvider caffeineProvider = CaffeineProvider.create();

        Pipeline pipeline = Pipeline
            .create(mongoProvider)
            .globalCache(redisProvider)
            .localCache(caffeineProvider, redisProvider)
            .build();

        OnlineTimeManager onlineTimeManager = new ObjectOnlineTimeManager(pipeline);
        UUID uuid = UuidUtil.fromName("DemoPlayer");

        onlineTimeManager.handleJoin(uuid);
        Thread.sleep(10000);
        onlineTimeManager.handleQuit(uuid);

        pipeline.closeProviders();
    }

}
