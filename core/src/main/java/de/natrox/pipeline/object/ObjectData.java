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

package de.natrox.pipeline.object;

import de.natrox.pipeline.document.DocumentData;
import de.natrox.pipeline.repository.Pipeline;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.UUID;

public abstract class ObjectData {

    private final transient Pipeline pipeline;
    private UUID uniqueId;

    public ObjectData(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public @NotNull UUID uniqueId() {
        return this.uniqueId;
    }

    /**
     * Executed after instantiation of the Object
     * Executed before Object is put into the repository
     */
    public void handleCreate() {

    }

    /**
     * Executed after an Updater updated the object
     *
     * @param before The DocumentData the object had before syncing
     */
    public void handleUpdate(@NotNull DocumentData before) {

    }

    /**
     * Executed before the object is deleted from the repository.
     */
    public void handleDelete() {

    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof ObjectData pipelineData))
            return false;

        return Objects.equals(this.uniqueId(), pipelineData.uniqueId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.uniqueId());
    }
}
