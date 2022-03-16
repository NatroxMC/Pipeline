package de.notion.pipeline.part;

import de.notion.common.system.SystemLoadable;
import de.notion.pipeline.datatype.PipelineData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface DataSynchronizer extends SystemLoadable {

    default @NotNull CompletableFuture<Boolean> synchronize(
            @NotNull DataSourceType source,
            @NotNull DataSourceType destination,
            @NotNull Class<? extends PipelineData> dataClass,
            @NotNull UUID objectUUID
    ) {
        return synchronize(source, destination, dataClass, objectUUID, null);
    }

    @NotNull
    CompletableFuture<Boolean> synchronize(
            @NotNull DataSourceType source,
            @NotNull DataSourceType destination,
            @NotNull Class<? extends PipelineData> dataClass,
            @NotNull UUID objectUUID,
            @Nullable Runnable callback
    );

    enum DataSourceType {
        LOCAL,
        GLOBAL_CACHE,
        GLOBAL_STORAGE
    }
}
