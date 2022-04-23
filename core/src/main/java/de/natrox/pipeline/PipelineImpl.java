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

package de.natrox.pipeline;

import de.natrox.common.validate.Check;
import de.natrox.pipeline.document.DocumentRepository;
import de.natrox.pipeline.document.DocumentRepositoryFactory;
import de.natrox.pipeline.object.ObjectData;
import de.natrox.pipeline.object.ObjectRepository;
import de.natrox.pipeline.object.ObjectRepositoryFactory;
import de.natrox.pipeline.part.StoreManager;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

final class PipelineImpl implements Pipeline {

    private final StoreManager storeManager;

    private final DocumentRepositoryFactory documentRepositoryFactory;
    private final ObjectRepositoryFactory objectRepositoryFactory;

    PipelineImpl(@NotNull PartBundle partBundle) {
        Check.notNull(partBundle, "partBundle");

        this.storeManager = partBundle.createStoreManager();

        this.documentRepositoryFactory = new DocumentRepositoryFactory();
        this.objectRepositoryFactory = new ObjectRepositoryFactory();
    }

    @Override
    public @NotNull DocumentRepository repository(@NotNull String name) {
        Check.notNull(name, "name");
        return documentRepositoryFactory.repository(name);
    }

    @Override
    public <T extends ObjectData> @NotNull ObjectRepository<T> repository(@NotNull Class<T> type) {
        return null;
    }

    @Override
    public void destroyRepository(@NotNull String name) {

    }

    @Override
    public <T extends ObjectData> void destroyRepository(@NotNull Class<T> type) {

    }

    @Override
    public @NotNull Set<String> listDocumentRepositories() {
        return null;
    }

    @Override
    public @NotNull Set<String> listObjectRepositories() {
        return null;
    }

    @Override
    public boolean isShutDowned() {
        return false;
    }

    @Override
    public void shutdown() {

    }

    public StoreManager storeManager() {
        return this.storeManager;
    }
}
