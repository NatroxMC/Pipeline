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

package de.natrox.pipeline.repository;

import de.natrox.common.container.Pair;
import de.natrox.common.validate.Check;
import de.natrox.pipeline.concurrent.LockService;
import de.natrox.pipeline.condition.Condition;
import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.document.serialize.DocumentSerializer;
import de.natrox.pipeline.find.FindOptions;
import de.natrox.pipeline.part.config.StorageConfig;
import de.natrox.pipeline.sort.SortEntry;
import de.natrox.pipeline.sort.SortOrder;
import de.natrox.pipeline.stream.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

final class DocumentRepositoryImpl implements DocumentRepository {

    private final String repositoryName;
    private final DocumentSerializer documentSerializer;
    private final RepositoryOptions.DocumentOptions options;

    private final Lock writeLock;
    private final Lock readLock;

    private PipelineStore pipelineStore;
    private PipelineMap pipelineMap;

    DocumentRepositoryImpl(String repositoryName, AbstractPipeline pipeline, PipelineStore pipelineStore, PipelineMap pipelineMap, LockService lockService, RepositoryOptions.DocumentOptions options) {
        this.repositoryName = repositoryName;
        this.pipelineStore = pipelineStore;
        this.pipelineMap = pipelineMap;
        this.documentSerializer = pipeline.documentMapper();
        this.options = options;
        this.readLock = lockService.getReadLock(repositoryName);
        this.writeLock = lockService.getWriteLock(repositoryName);
    }

    @Override
    public @NotNull Optional<DocumentData> get(@NotNull UUID uniqueId) {
        Check.notNull(uniqueId, "uniqueId");

        try {
            this.readLock.lock();
            this.checkOpened();
            byte[] data = this.pipelineMap.get(uniqueId);
            if (data == null)
                return Optional.empty();
            return Optional.of(this.documentSerializer.read(data));
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public @NotNull DocumentStream find(@NotNull FindOptions findOptions) {
        Check.notNull(findOptions, "findOptions");

        try {
            this.readLock.lock();
            this.checkOpened();
            PipeStream<Pair<UUID, DocumentData>> stream = PipeStream.fromIterable(
                this.pipelineMap
                    .entries()
                    .entrySet()
                    .stream()
                    .map(entry -> Pair.of(entry.getKey(), this.documentSerializer.read(entry.getValue())))
                    .collect(Collectors.toList())
            );

            Condition condition = findOptions.condition();
            if (condition != null) {
                stream = new ConditionalStream(condition, stream);
            }

            SortEntry sortBy = findOptions.sortBy();
            if (sortBy != null) {
                List<Pair<String, SortOrder>> blockingSortOrder = sortBy.sortingOrders();
                if (!blockingSortOrder.isEmpty()) {
                    stream = new SortedDocumentStream(blockingSortOrder, stream);
                }
            }

            if (findOptions.limit() != -1 || findOptions.skip() != -1) {
                long limit = findOptions.limit() == -1 ? Long.MAX_VALUE : findOptions.limit();
                long skip = findOptions.skip() == -1 ? 0 : findOptions.skip();
                stream = new BoundedStream<>(skip, limit, stream);
            }

            return new DocumentStream(stream);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void insert(@NotNull UUID uniqueId, @NotNull DocumentData document, QueryStrategy @NotNull ... strategies) {
        Check.notNull(uniqueId, "uniqueId");
        Check.notNull(document, "document");

        try {
            this.writeLock.lock();
            this.checkOpened();
            DocumentData newDoc = document.clone();

            this.pipelineMap.put(uniqueId, this.documentSerializer.write(newDoc), strategies);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean exists(@NotNull UUID uniqueId, QueryStrategy @NotNull ... strategies) {
        Check.notNull(uniqueId, "uniqueId");
        Check.notNull(strategies, "strategies");
        Check.argCondition(strategies.length <= 0, "strategies");
        try {
            this.readLock.lock();
            this.checkOpened();
            return this.pipelineMap.contains(uniqueId, strategies);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void remove(@NotNull UUID uniqueId, QueryStrategy @NotNull ... strategies) {
        Check.notNull(uniqueId, "uniqueId");
        Check.notNull(strategies, "strategies");
        Check.argCondition(strategies.length <= 0, "strategies");
        try {
            this.writeLock.lock();
            this.checkOpened();
            this.pipelineMap.remove(uniqueId, Set.of(strategies));
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public @NotNull String name() {
        return this.repositoryName;
    }

    @Override
    public void clear() {
        try {
            this.writeLock.lock();
            this.checkOpened();
            this.pipelineMap.clear();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            this.writeLock.lock();
            if (this.pipelineStore == null || this.pipelineMap == null) {
                return;
            }

            this.pipelineStore.closeMap(this.repositoryName);
            this.pipelineMap.close();

            this.pipelineStore = null;
            this.pipelineMap = null;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean isDropped() {
        return !this.pipelineStore.hasMap(this.repositoryName);
    }

    @Override
    public void drop() {
        try {
            this.writeLock.lock();
            this.checkOpened();

            this.pipelineStore.removeMap(this.repositoryName);
            this.pipelineStore.closeMap(this.repositoryName);

            this.pipelineStore = null;
            this.pipelineMap = null;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean isOpen() {
        try {
            this.readLock.lock();
            return this.pipelineStore != null && !this.pipelineStore.isClosed() && !this.isDropped();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long size() {
        try {
            this.readLock.lock();
            this.checkOpened();
            return this.pipelineMap.size();
        } finally {
            this.readLock.unlock();
        }
    }

    private void checkOpened() {
        if (this.isOpen())
            return;
        throw new RuntimeException("Repository is closed");
    }

    final static class BuilderImpl extends AbstractRepositoryBuilder<DocumentRepository, DocumentRepository.Builder> implements DocumentRepository.Builder {

        private final DocumentRepositoryFactory factory;
        private final String name;

        BuilderImpl(DocumentRepositoryFactory factory, String name, StorageConfig config) {
            super(config);
            this.factory = factory;
            this.name = name;
        }

        @Override
        public DocumentRepository build() {
            return this.factory.createRepository(this.name, new RepositoryOptions.DocumentOptions(
                this.storageConfig, this.useGlobalCache, this.globalCacheConfig, this.useLocalCache, this.localCacheConfig
            ));
        }
    }
}
