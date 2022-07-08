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

import de.natrox.pipeline.document.DocumentData;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.UUID;

@ApiStatus.Experimental
public sealed interface DocumentRepository extends Repository<DocumentData> permits DocumentRepositoryImpl {

    void insert(@NotNull UUID uniqueId, @NotNull DocumentData document, QueryStrategy @NotNull ... strategies);

    @NotNull Optional<DocumentData> get(@NotNull UUID uniqueId);

    @NotNull String name();

    @ApiStatus.Experimental
    sealed interface Builder extends Repository.Builder<DocumentRepository, Builder> permits DocumentRepositoryImpl.BuilderImpl {

    }

}
