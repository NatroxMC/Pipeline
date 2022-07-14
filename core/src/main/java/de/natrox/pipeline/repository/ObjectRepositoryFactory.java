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

import de.natrox.pipeline.object.ObjectData;
import de.natrox.pipeline.object.annotation.AnnotationResolver;
import de.natrox.serialize.exception.SerializeException;
import de.natrox.serialize.objectmapping.ObjectMapper;
import org.jetbrains.annotations.ApiStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ApiStatus.Internal
final class ObjectRepositoryFactory {

    private final AbstractPipeline pipeline;
    private final DocumentRepositoryFactory documentRepositoryFactory;
    private final Map<String, ObjectRepository<? extends ObjectData>> repositoryMap;
    private final Lock lock;

    ObjectRepositoryFactory(AbstractPipeline pipeline, DocumentRepositoryFactory documentRepositoryFactory) {
        this.pipeline = pipeline;
        this.documentRepositoryFactory = documentRepositoryFactory;
        this.repositoryMap = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    @SuppressWarnings("unchecked")
    public <T extends ObjectData> ObjectRepository<T> repository(Class<T> type) {
        String name = AnnotationResolver.identifier(type);

        try {
            this.lock.lock();
            if (this.repositoryMap.containsKey(name)) {
                ObjectRepository<T> repository = (ObjectRepository<T>) this.repositoryMap.get(name);
                if (!repository.isDropped() && repository.isOpen()) {
                    return repository;
                }
                this.repositoryMap.remove(name);
            }
        } finally {
            this.lock.unlock();
        }

        //TODO: message/reason
        throw new IllegalStateException();
    }

    public <T extends ObjectData> ObjectRepository<T> createRepository(Class<T> type, RepositoryOptions.ObjectOptions<T> options) {
        String name = AnnotationResolver.identifier(type);

        if (this.repositoryMap.containsKey(name)) {
            ObjectRepository<T> repository = this.repository(type);
            repository.close();
            DocumentRepositoryImpl documentRepository = this.documentRepositoryFactory.repository(name);
            documentRepository.close();
        }

        try {
            ObjectMapper<T> objectMapper = ObjectMapper.factory().get(type);
            DocumentRepositoryImpl documentRepository = this.documentRepositoryFactory.createRepository(name, options);
            ObjectRepository<T> repository = new ObjectRepositoryImpl<>(this.pipeline, type, objectMapper, documentRepository, options);
            this.repositoryMap.put(name, repository);

            return repository;
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        try {
            this.lock.lock();
            for (ObjectRepository<?> repository : this.repositoryMap.values()) {
                repository.close();
            }
            this.repositoryMap.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
