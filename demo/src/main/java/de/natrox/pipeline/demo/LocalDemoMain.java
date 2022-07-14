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

package de.natrox.pipeline.demo;

import de.natrox.common.util.UuidUtil;
import de.natrox.pipeline.bin.BinConfig;
import de.natrox.pipeline.bin.BinProvider;
import de.natrox.pipeline.caffeine.CaffeineProvider;
import de.natrox.pipeline.demo.onlinetime.DocumentOnlineTimeManager;
import de.natrox.pipeline.demo.onlinetime.OnlineTimeManager;
import de.natrox.pipeline.repository.Pipeline;

import java.nio.file.Path;
import java.util.UUID;

public final class LocalDemoMain {

    public static void main(String[] args) throws InterruptedException {
        BinConfig binConfig = BinConfig
            .create()
            .setPath(Path.of("storage"));
        BinProvider binProvider = BinProvider.of(binConfig);

        CaffeineProvider caffeineProvider = CaffeineProvider.create();

        Pipeline pipeline = Pipeline
            .create(binProvider)
            .localCache(caffeineProvider)
            .build();

        OnlineTimeManager onlineTimeManager = new DocumentOnlineTimeManager(pipeline);
        UUID uuid = UuidUtil.fromName("DemoPlayer");

        onlineTimeManager.handleJoin(uuid);
        Thread.sleep(10000);
        onlineTimeManager.handleQuit(uuid);

        pipeline.closeProviders();
    }

}
