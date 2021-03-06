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

package de.natrox.pipeline.part.config;

import org.jetbrains.annotations.NotNull;

public sealed interface StorageConfig extends StoreMapConfig permits StoreMapConfigImpl.StorageConfigImpl {

    static @NotNull StorageConfig.Builder builder() {
        return new StoreMapConfigBuilderImpl.StorageBuilder();
    }

    static @NotNull StorageConfig defaults() {
        return StoreMapConfigImpl.StorageConfigImpl.DEFAULT;
    }

    interface Builder extends StoreMapConfig.Builder<StorageConfig, StorageConfig.Builder> {

    }
}
