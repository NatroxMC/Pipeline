package de.natrox.pipeline.json;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.natrox.common.logger.LogManager;
import de.natrox.common.logger.Logger;
import de.natrox.pipeline.Pipeline;
import de.natrox.pipeline.annotation.resolver.AnnotationResolver;
import de.natrox.pipeline.datatype.PipelineData;
import de.natrox.pipeline.part.storage.GlobalStorage;
import jodd.io.FileNameUtil;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class JsonStorage implements GlobalStorage {

    private final static Logger LOGGER = LogManager.logger(JsonStorage.class);

    private final Gson gson;
    private final Path directory;

    public JsonStorage(Pipeline pipeline, String path) {
        this.gson = pipeline.gson();
        this.directory = Paths.get(path);

        LOGGER.debug("Json storage initialized"); //DEBUG
    }

    @Override
    public JsonObject loadData(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        Objects.requireNonNull(objectUUID, "objectUUID can't be null!");
        try {
            return loadFromFile(dataClass, objectUUID);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean dataExist(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        Objects.requireNonNull(objectUUID, "objectUUID can't be null!");
        return Files.exists(savedFile(dataClass, objectUUID));
    }

    @Override
    public void saveData(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID, @NotNull JsonObject dataToSave) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        Objects.requireNonNull(objectUUID, "objectUUID can't be null!");
        Objects.requireNonNull(dataToSave, "dataToSave can't be null!");
        try {
            saveJsonToFile(dataClass, objectUUID, dataToSave);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean removeData(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        Objects.requireNonNull(objectUUID, "objectUUID can't be null!");
        if (!dataExist(dataClass, objectUUID))
            return false;
        try {
            Files.deleteIfExists(savedFile(dataClass, objectUUID));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public @NotNull Collection<UUID> savedUUIDs(@NotNull Class<? extends PipelineData> dataClass) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        return data(dataClass).keySet();
    }

    @Override
    public @NotNull Map<UUID, JsonObject> data(@NotNull Class<? extends PipelineData> dataClass) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        var parentFolder = parent(dataClass);
        if (!parentFolder.toFile().exists())
            return Map.of();
        try {
            var data = new HashMap<UUID, JsonObject>();

            Files.walk(parentFolder, 1)
                .skip(1)
                .filter(path1 -> FileNameUtil.getExtension(path1.getFileName().toString()).equals(".json"))
                .map(path1 -> FileNameUtil.getBaseName(path1.toString()))
                .map(UUID::fromString)
                .forEach(uuid -> {
                    var jsonObject = loadData(dataClass, uuid);
                    if (jsonObject == null)
                        return;

                    data.put(uuid, jsonObject);
                });

            return data;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Map.of();
    }

    private void saveJsonToFile(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID, @NotNull JsonObject dataToSave) throws IOException {
        if (dataToSave.isJsonNull())
            return;
        var path = savedFile(dataClass, objectUUID);

        var file = new File(path.toUri());
        if (!file.exists()) {
            if (!file.getParentFile().mkdirs() || !file.createNewFile())
                throw new RuntimeException("Could not create files for JsonFileStorage [" + path + "]");
        }
        try (var writer = new FileWriter(file)) {
            gson.toJson(dataToSave, writer);
        }
    }

    private JsonObject loadFromFile(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID) throws IOException {
        var path = savedFile(dataClass, objectUUID);
        var file = new File(path.toUri());
        if (!file.exists())
            throw new RuntimeException("SavedFile does not exist for " + dataClass.getSimpleName() + ":" + objectUUID);
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toFile()))) {
            return JsonParser.parseReader(bufferedReader).getAsJsonObject();
        }
    }

    private Path savedFile(@NotNull Class<? extends PipelineData> dataClass, @NotNull UUID objectUUID) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        Objects.requireNonNull(objectUUID, "objectUUID can't be null!");
        return Paths.get(parent(dataClass).toString(), objectUUID + ".json");
    }

    private Path parent(@NotNull Class<? extends PipelineData> dataClass) {
        Objects.requireNonNull(dataClass, "dataClass can't be null!");
        var storageIdentifier = AnnotationResolver.storageIdentifier(dataClass);

        return Paths.get(directory.toString(), storageIdentifier);
    }
}