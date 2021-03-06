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

package de.natrox.pipeline.condition;

import de.natrox.common.container.Pair;
import de.natrox.pipeline.document.DocumentData;

import java.util.UUID;

final class NotEqualsCondition extends ComparableCondition {

    private final Condition condition;

    NotEqualsCondition(String field, Object value) {
        super(field, value);
        this.condition = new EqualsCondition(field, value).not();
    }

    @Override
    public boolean apply(Pair<UUID, DocumentData> element) {
        return this.condition.apply(element);
    }
}
