package de.natrox.pipeline.cleanup;

import de.natrox.common.logger.LogManager;
import de.natrox.common.logger.Logger;
import de.natrox.common.runnable.CatchingRunnable;
import de.natrox.pipeline.PipelineImpl;
import de.natrox.pipeline.annotation.resolver.AnnotationResolver;
import de.natrox.pipeline.config.PipelineRegistry;
import de.natrox.pipeline.part.local.LocalCache;

import java.util.UUID;

public final class CleanUpTask implements Runnable {

    private final static Logger LOGGER = LogManager.logger(CleanUpTask.class);

    private final PipelineImpl pipelineImpl;
    private final PipelineRegistry registry;
    private final LocalCache localCache;

    public CleanUpTask(PipelineImpl pipelineImpl) {
        this.pipelineImpl = pipelineImpl;
        this.registry = pipelineImpl.registry();
        this.localCache = pipelineImpl.localCache();
    }

    @Override
    public void run() {
        for (var dataClass : registry.dataClasses()) {
            AnnotationResolver.cleanUp(dataClass).ifPresent(cleanUp -> {
                for (var uuid : localCache.savedUUIDs(dataClass)) {
                    var data = localCache.data(dataClass, uuid);
                    if (data == null)
                        return;
                    if ((System.currentTimeMillis() - data.lastUse()) < cleanUp.timeUnit().toMillis(cleanUp.time()))
                        return;

                    LOGGER.debug("Cleaning up " + dataClass.getSimpleName() + " with uuid " + uuid.toString());
                    data.onCleanUp();
                    pipelineImpl.cleanUpData(dataClass, data.objectUUID(), null);
                }
            });
        }
    }
}