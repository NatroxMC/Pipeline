/*
 * Copyright 2020-2022 NatroxMC team
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

package de.natrox.pipeline.part.memory;

import de.natrox.common.container.Pair;
import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.part.StoreMap;
import de.natrox.pipeline.stream.PipeStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InMemoryMap implements StoreMap {

    private final Map<UUID, DocumentData> documentMap = new HashMap<>();

    @Override
    public @Nullable DocumentData get(@NotNull UUID uniqueId) {
        return documentMap.get(uniqueId);
    }

    @Override
    public void put(@NotNull UUID uniqueId, @NotNull DocumentData documentData) {
        documentMap.put(uniqueId, documentData);
    }

    @Override
    public boolean contains(@NotNull UUID uniqueId) {
        return documentMap.containsKey(uniqueId);
    }

    @Override
    public @NotNull PipeStream<UUID> keys() {
        return PipeStream.fromIterable(documentMap.keySet());
    }

    @Override
    public @NotNull PipeStream<DocumentData> values() {
        return PipeStream.fromIterable(documentMap.values());
    }

    @Override
    public @NotNull PipeStream<Pair<UUID, DocumentData>> entries() {
        return PipeStream.fromMap(documentMap);
    }

    @Override
    public void remove(@NotNull UUID uniqueId) {
        documentMap.remove(uniqueId);
    }

    @Override
    public void clear() {
        documentMap.clear();
    }

    @Override
    public long size() {
        return documentMap.size();
    }
}
