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

package de.natrox.pipeline.h2;

import de.natrox.common.validate.Check;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

public final class H2Config {

    //TODO: Find better solution ?? Maybe use Path
    String directory;

    H2Config() {

    }

    public static @NotNull H2Config create() {
        return new H2Config();
    }

    public @NotNull H2Config setPath(@NotNull Path directory) {
        Check.notNull(directory, "directory");
        this.directory = directory.toFile().getAbsolutePath();
        return this;
    }
}
