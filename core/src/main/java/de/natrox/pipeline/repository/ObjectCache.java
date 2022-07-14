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

import de.natrox.eventbus.EventBus;
import de.natrox.eventbus.EventListener;
import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.object.InstanceCreator;
import de.natrox.pipeline.object.ObjectData;
import de.natrox.pipeline.object.type.TypeInstanceCreator;
import de.natrox.pipeline.part.updater.Updater;
import de.natrox.pipeline.part.updater.event.DocumentUpdateEvent;
import de.natrox.serialize.exception.SerializeException;
import de.natrox.serialize.objectmapping.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

final class ObjectCache<T extends ObjectData> {

    private final Pipeline pipeline;
    private final Updater updater;

    private final ObjectMapper<T> objectMapper;
    private final Class<T> type;
    private final String mapName;
    private final InstanceCreator<T> instanceCreator;
    private final Map<UUID, T> cache;

    ObjectCache(AbstractPipeline pipeline, ObjectRepositoryImpl<T> repository, ObjectMapper<T> objectMapper, RepositoryOptions.ObjectOptions<T> options) {
        this.pipeline = pipeline;
        this.updater = pipeline.updater();
        this.objectMapper = objectMapper;
        this.type = repository.type();
        this.mapName = repository.mapName();
        this.cache = new ConcurrentHashMap<>();
        this.instanceCreator = options.instanceCreator() != null ? options.instanceCreator() : TypeInstanceCreator.create(this.type);
        this.registerListeners();
    }

    private void registerListeners() {
        if (this.updater == null)
            return;

        EventBus eventBus = this.updater.eventBus();
        eventBus.register(
            EventListener
                .builder(DocumentUpdateEvent.class)
                .condition(event -> event.repositoryName().equals(this.mapName))
                .handler(this::updateData)
                .build()
        );
    }

    private void updateData(DocumentUpdateEvent event) {
        this.get(event.documentId()).ifPresent(data -> this.updateData(data, event.documentData()));
    }

    private void updateData(T data, DocumentData update) {
        try {
            DocumentData before = DocumentData.create(this.objectMapper.save(data));
            this.objectMapper.load(data, update.asMap());
            data.handleUpdate(before);
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull Optional<T> get(@NotNull UUID uniqueId) {
        return Optional.ofNullable(this.cache.get(uniqueId));
    }

    @NotNull T getOrCreate(@NotNull UUID uniqueId, @Nullable InstanceCreator<T> instanceCreator) {
        if (!this.cache.containsKey(uniqueId)) {
            this.cache.put(uniqueId, this.create(uniqueId, instanceCreator));
        }
        return this.cache.get(uniqueId);
    }

    private @NotNull T create(@NotNull UUID uniqueId, @Nullable InstanceCreator<T> instanceCreator) {
        T instance;
        InstanceCreator<T> creator = instanceCreator != null ? instanceCreator : this.instanceCreator;
        try {
            instance = creator.create(this.pipeline);
        } catch (Throwable throwable) {
            throw new RuntimeException("Error while creating instance of class " + this.type.getSimpleName(), throwable);
        }

        DocumentData documentData = DocumentData.create();
        documentData.append("uniqueId", uniqueId);

        try {
            this.objectMapper.load(instance, documentData.asMap());
            return instance;
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

}
