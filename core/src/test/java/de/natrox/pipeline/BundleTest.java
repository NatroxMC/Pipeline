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

import de.natrox.pipeline.part.storage.provider.LocalStorageProvider;
import org.junit.jupiter.api.Test;

public class BundleTest {

    @Test
    public void test(LocalStorageProvider localStorageProvider) {
        PartBundle bundle = PartBundle.local(localStorageProvider);

        Pipeline pipeline = Pipeline
            .builder()
            .bundle(bundle)
            .build();
    }

}
