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

import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.repository.DocumentRepository;
import de.natrox.pipeline.repository.Pipeline;
import de.natrox.pipeline.repository.QueryStrategy;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ConstantConditions")
public final class DocumentOnlineTimeManager implements OnlineTimeManager {

    private final DocumentRepository repository;

    public DocumentOnlineTimeManager(@NotNull Pipeline pipeline) {
        this.repository = pipeline
            .buildRepository("OnlineTimeData")
            .useGlobalCache(builder -> builder.expireAfterWrite(2, TimeUnit.HOURS).expireAfterAccess(2, TimeUnit.HOURS))
            .useLocalCache(builder -> builder.expireAfterWrite(2, TimeUnit.HOURS).expireAfterAccess(2, TimeUnit.HOURS))
            .build();
    }

    public void handleJoin(@NotNull UUID uuid) {
        DocumentData documentData = this.repository.get(uuid).orElseGet(() -> {
            DocumentData data = DocumentData.create();
            data.append("onlineTime", 0L);
            return data;
        });

        documentData.append("lastJoin", System.currentTimeMillis());
        this.repository.insert(uuid, documentData);
    }

    public void handleQuit(@NotNull UUID uuid) {
        this.repository.get(uuid).ifPresent(documentData -> {
            long onlineTime = documentData.get("onlineTime", long.class);
            long lastJoin = documentData.get("lastJoin", long.class);

            onlineTime += System.currentTimeMillis() - lastJoin;
            documentData.append("onlineTime", onlineTime);
            this.repository.insert(uuid, documentData, QueryStrategy.GLOBAL_STORAGE);
            this.repository.remove(uuid, QueryStrategy.LOCAL_CACHE, QueryStrategy.GLOBAL_CACHE);
        });
    }

}
