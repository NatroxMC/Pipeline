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

package de.natrox.pipeline.demo.onlinetime;

import de.natrox.pipeline.object.ObjectData;
import de.natrox.pipeline.object.annotation.Properties;
import de.natrox.pipeline.repository.ObjectRepository;
import de.natrox.pipeline.repository.Pipeline;
import de.natrox.pipeline.repository.QueryStrategy;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class ObjectOnlineTimeManager implements OnlineTimeManager {

    private final ObjectRepository<OnlineTimeData> repository;

    public ObjectOnlineTimeManager(@NotNull Pipeline pipeline) {
        this.repository = pipeline
            .buildRepository(OnlineTimeData.class)
            .useGlobalCache(builder -> builder.expireAfterWrite(2, TimeUnit.HOURS).expireAfterAccess(2, TimeUnit.HOURS))
            .useLocalCache(builder -> builder.expireAfterWrite(2, TimeUnit.HOURS).expireAfterAccess(2, TimeUnit.HOURS))
            .build();
    }

    public void handleJoin(@NotNull UUID uuid) {
        OnlineTimeData onlineTimeData = this.repository.loadOrCreate(uuid);

        onlineTimeData.setLastJoin(System.currentTimeMillis());
        this.repository.save(onlineTimeData);
    }

    public void handleQuit(@NotNull UUID uuid) {
        this.repository.load(uuid).ifPresent(onlineTimeData -> {
            long onlineTime = onlineTimeData.onlineTime();
            long lastJoin = onlineTimeData.lastJoin();

            onlineTime += System.currentTimeMillis() - lastJoin;
            onlineTimeData.setOnlineTime(onlineTime);
            this.repository.save(onlineTimeData, QueryStrategy.GLOBAL_STORAGE);
            this.repository.remove(uuid, QueryStrategy.LOCAL_CACHE, QueryStrategy.GLOBAL_CACHE);
        });
    }

    @Properties(identifier = "OnlineTimeData")
    final static class OnlineTimeData extends ObjectData {

        private long onlineTime;
        private long lastJoin;

        public OnlineTimeData(Pipeline pipeline) {
            super(pipeline);
        }

        @Override
        public void handleCreate() {
            this.onlineTime = 0L;
        }

        public long onlineTime() {
            return this.onlineTime;
        }

        public void setOnlineTime(long onlineTime) {
            this.onlineTime = onlineTime;
        }

        public long lastJoin() {
            return this.lastJoin;
        }

        public void setLastJoin(long lastJoin) {
            this.lastJoin = lastJoin;
        }
    }
}
