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

import de.natrox.common.validate.Check;
import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.find.FindOptions;
import de.natrox.pipeline.object.InstanceCreator;
import de.natrox.pipeline.object.ObjectData;
import de.natrox.pipeline.part.config.StorageConfig;
import de.natrox.pipeline.stream.Cursor;
import de.natrox.pipeline.stream.DocumentStream;
import de.natrox.serialize.exception.SerializeException;
import de.natrox.serialize.objectmapping.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

final class ObjectRepositoryImpl<T extends ObjectData> implements ObjectRepository<T> {

    private final Class<T> type;
    private final DocumentRepositoryImpl documentRepository;
    private final String mapName;
    private final ObjectMapper<T> objectMapper;
    private final ObjectCache<T> objectCache;

    ObjectRepositoryImpl(AbstractPipeline pipeline, Class<T> type, ObjectMapper<T> objectMapper, DocumentRepositoryImpl documentRepository, RepositoryOptions.ObjectOptions<T> options) {
        this.type = type;
        this.documentRepository = documentRepository;
        this.mapName = documentRepository.name();
        this.objectMapper = objectMapper;
        this.objectCache = new ObjectCache<>(pipeline, this, this.objectMapper, options);
    }

    @Override
    public @NotNull Optional<T> load(@NotNull UUID uniqueId, @Nullable InstanceCreator<T> instanceCreator) {
        Check.notNull(uniqueId, "uniqueId");
        // Instantiate T as fast as possible so that the Updater can still apply updates while loading a record
        T data = this.objectCache.getOrCreate(uniqueId, instanceCreator);
        return this.documentRepository.get(uniqueId).map(document -> {
            try {
                this.objectMapper.load(data, document.asMap());
                return data;
            } catch (SerializeException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public @NotNull T loadOrCreate(@NotNull UUID uniqueId, @Nullable InstanceCreator<T> instanceCreator) {
        Check.notNull(uniqueId, "uniqueId");
        return this.load(uniqueId, instanceCreator).orElseGet(() -> this.create(uniqueId, instanceCreator));
    }

    private T create(UUID uniqueId, @Nullable InstanceCreator<T> instanceCreator) {
        Check.notNull(uniqueId, "uniqueId");
        try {
            T data = this.objectCache.getOrCreate(uniqueId, instanceCreator);
            data.handleCreate();
            DocumentData documentData = DocumentData.create(this.objectMapper.save(data));
            this.documentRepository.insert(uniqueId, documentData);
            return data;
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public @NotNull Cursor<T> find(@NotNull FindOptions findOptions, @Nullable InstanceCreator<T> instanceCreator) {
        Check.notNull(findOptions, "findOptions");
        DocumentStream documentCursor = this.documentRepository.find(findOptions);
        return new ObjectStream<>(this, instanceCreator, documentCursor.asPairStream());
    }

    @Override
    public void save(@NotNull T objectData, QueryStrategy @NotNull ... strategies) {
        Check.notNull(objectData, "objectData");
        try {
            this.documentRepository.insert(objectData.uniqueId(), DocumentData.create(this.objectMapper.save(objectData)), strategies);
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists(@NotNull UUID uniqueId, QueryStrategy @NotNull ... strategies) {
        Check.notNull(uniqueId, "uniqueId");
        return this.documentRepository.exists(uniqueId, strategies);
    }

    @Override
    public void remove(@NotNull UUID uniqueId, QueryStrategy @NotNull ... strategies) {
        Check.notNull(uniqueId, "uniqueId");
        //TODO: Only delete if strategies contains LOCAL_CACHE
        Optional<T> optional = this.objectCache.get(uniqueId);
        optional.ifPresent(ObjectData::handleDelete);
        this.documentRepository.remove(uniqueId, strategies);
    }

    @Override
    public @NotNull Class<T> type() {
        return this.type;
    }

    public @NotNull String mapName() {
        return this.mapName;
    }

    @Override
    public @NotNull DocumentRepository documentRepository() {
        return this.documentRepository;
    }

    @Override
    public void clear() {
        this.documentRepository.clear();
    }

    @Override
    public void drop() {
        this.documentRepository.drop();
    }

    @Override
    public boolean isDropped() {
        return this.documentRepository.isDropped();
    }

    @Override
    public void close() {
        this.documentRepository.close();
    }

    @Override
    public boolean isOpen() {
        return this.documentRepository.isOpen();
    }

    @Override
    public long size() {
        return this.documentRepository.size();
    }

    T instantiate(UUID uniqueId, DocumentData document, @Nullable InstanceCreator<T> instanceCreator) {
        try {
            T data = this.objectCache.getOrCreate(uniqueId, instanceCreator);
            this.objectMapper.load(data, document.asMap());
            return data;
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    final static class BuilderImpl<T extends ObjectData> extends AbstractRepositoryBuilder<ObjectRepository<T>, ObjectRepository.Builder<T>> implements ObjectRepository.Builder<T> {

        private final ObjectRepositoryFactory factory;
        private final Class<T> type;
        private InstanceCreator<T> instanceCreator;

        BuilderImpl(ObjectRepositoryFactory factory, Class<T> type, StorageConfig config) {
            super(config);
            this.factory = factory;
            this.type = type;
        }

        @Override
        public @NotNull Builder<T> instanceCreator(@NotNull InstanceCreator<T> instanceCreator) {
            Check.notNull(instanceCreator, "instanceCreator");
            this.instanceCreator = instanceCreator;
            return this;
        }

        @Override
        public ObjectRepository<T> build() {
            return this.factory.createRepository(this.type, new RepositoryOptions.ObjectOptions<>(
                this.storageConfig, this.useGlobalCache, this.globalCacheConfig, this.useLocalCache, this.localCacheConfig, this.instanceCreator
            ));
        }
    }
}
