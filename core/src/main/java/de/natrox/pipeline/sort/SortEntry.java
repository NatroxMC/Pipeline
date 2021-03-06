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

package de.natrox.pipeline.sort;

import de.natrox.common.container.Pair;
import org.jetbrains.annotations.ApiStatus;

import java.io.Serializable;
import java.util.List;

@ApiStatus.Experimental
@ApiStatus.Internal
public sealed interface SortEntry extends Comparable<SortEntry>, Serializable permits SortEntryImpl {

    List<String> fieldNames();

    List<Pair<String, SortOrder>> sortingOrders();

}
