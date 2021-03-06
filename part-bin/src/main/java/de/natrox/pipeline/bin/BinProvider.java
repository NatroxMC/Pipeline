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

package de.natrox.pipeline.bin;

import de.natrox.common.validate.Check;
import de.natrox.pipeline.part.provider.LocalStorageProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

public sealed interface BinProvider extends LocalStorageProvider permits BinProviderImpl {

    static @NotNull BinProvider of(@NotNull Path directory) {
        Check.notNull(directory, "directory");
        return new BinProviderImpl(directory);
    }

    static @NotNull BinProvider of(@NotNull String directory) {
        Check.notNull(directory, "directory");
        return of(Path.of(directory));
    }

    static @NotNull BinProvider of(@NotNull BinConfig config) {
        Check.notNull(config, "config");
        return of(config.directory);
    }
}
