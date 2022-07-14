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

package de.natrox.pipeline.document;

import de.natrox.common.container.Pair;
import de.natrox.common.validate.Check;
import de.natrox.pipeline.util.Iterables;
import de.natrox.pipeline.util.ObjectUtil;
import de.natrox.pipeline.util.Strings;
import de.natrox.serialize.Deserializer;
import de.natrox.serialize.SerializerCollection;
import de.natrox.serialize.exception.CoercionFailedException;
import de.natrox.serialize.exception.SerializeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.text.MessageFormat;
import java.util.*;

public final class DocumentDataImpl extends HashMap<String, Object> implements DocumentData {

    private final static String FIELD_SEPARATOR = ".";

    DocumentDataImpl() {
        super();
    }

    DocumentDataImpl(Map<String, Object> objectMap) {
        super(objectMap);
    }

    @Override
    public @NotNull DocumentData append(@NotNull String field, @NotNull Object value) {
        Check.argCondition(Strings.isNullOrEmpty(field), "field is empty or null key");

        if (this.isEmbedded(field)) {
            String regex = MessageFormat.format("\\{0}", FIELD_SEPARATOR);
            String[] splits = field.split(regex);
            this.deepPut(splits, value);
        } else {
            super.put(field, value);
        }
        return this;
    }

    @Override
    public @Nullable Object get(@NotNull String field) {
        Check.notNull(field, "field");
        if (this.isEmbedded(field) && !this.containsKey(field))
            return this.deepGet(field);
        return super.get(field);
    }

    @Override
    public <T> @Nullable T get(@NotNull String field, @NotNull Class<T> type) {
        Check.notNull(type, "type");
        Deserializer<T> serial = SerializerCollection.defaults().get(type);
        if(serial == null) {
            //TODO: message/reason
            throw new RuntimeException();
        }

        Object value = this.get(field);

        if(value == null) {
            return null;
        }

        try {
            return serial.deserialize(value, type);
        } catch (SerializeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public @NotNull Set<String> fields() {
        return this.getFieldsInternal("");
    }

    @Override
    public void remove(@NotNull String field) {
        Check.notNull(field, "field");
        if (this.isEmbedded(field)) {
            String regex = MessageFormat.format("\\{0}", FIELD_SEPARATOR);
            String[] splits = field.split(regex);
            this.deepRemove(splits);
        } else {
            super.remove(field);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull DocumentData clone() {
        Map<String, Object> cloned = (Map<String, Object>) super.clone();

        for (Map.Entry<String, Object> entry : cloned.entrySet()) {
            if (entry.getValue() instanceof DocumentData value) {
                DocumentData clonedValue = value.clone();
                cloned.put(entry.getKey(), clonedValue);
            }
        }
        return new DocumentDataImpl(cloned);
    }

    @Override
    public @NotNull DocumentData merge(@NotNull DocumentData document) {
        Check.notNull(document, "document");
        if (document instanceof DocumentDataImpl doc) {
            super.putAll(doc);
        }
        return this;
    }

    @Override
    public boolean containsKey(@NotNull String key) {
        Check.notNull(key, "key");
        return super.containsKey(key);
    }

    @Override
    public @NotNull Map<String, Object> asMap() {
        return this;
    }

    @Override
    public boolean equals(Object other) {
        Check.notNull(other, "other");
        if (other == this)
            return true;

        if (!(other instanceof DocumentDataImpl m))
            return false;

        if (m.size() != size())
            return false;

        try {
            for (Map.Entry<String, Object> e : this.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key)))
                        return false;
                } else {
                    if (!Objects.deepEquals(value, m.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public @NotNull Iterator<Pair<String, Object>> iterator() {
        return new MapPairIterator(super.entrySet().iterator());
    }

    private Set<String> getFieldsInternal(String prefix) {
        Set<String> fields = new HashSet<>();

        for (Pair<String, Object> entry : this) {
            Object value = entry.second();
            if (value instanceof DocumentDataImpl document) {
                if (Strings.isNullOrEmpty(prefix)) {
                    fields.addAll(document.getFieldsInternal(entry.first()));
                } else {
                    fields.addAll(document.getFieldsInternal(prefix
                        + FIELD_SEPARATOR + entry.first()));
                }
            } else if (!(value instanceof Iterable)) {
                if (Strings.isNullOrEmpty(prefix)) {
                    fields.add(entry.first());
                } else {
                    fields.add(prefix + FIELD_SEPARATOR + entry.first());
                }
            }
        }
        return fields;
    }

    private Object deepGet(String field) {
        if (this.isEmbedded(field)) {
            return this.getByEmbeddedKey(field);
        } else {
            return null;
        }
    }

    private void deepPut(String[] splits, Object value) {
        if (splits.length == 0) {
            throw new RuntimeException("invalid key provided");
        }
        String key = splits[0];
        if (splits.length == 1) {
            this.append(key, value);
        } else {
            Object val = get(key);

            String[] remaining = Arrays.copyOfRange(splits, 1, splits.length);

            if (val instanceof DocumentDataImpl document) {
                document.deepPut(remaining, value);
            } else if (val == null) {
                DocumentDataImpl subDoc = new DocumentDataImpl();
                subDoc.deepPut(remaining, value);

                this.append(key, subDoc);
            }
        }
    }

    private void deepRemove(String[] splits) {
        if (splits.length == 0) {
            throw new RuntimeException("invalid key provided");
        }
        String key = splits[0];
        if (splits.length == 1) {
            this.remove(key);
        } else {
            Object val = this.get(key);

            String[] remaining = Arrays.copyOfRange(splits, 1, splits.length);

            if (val instanceof DocumentDataImpl subDoc) {
                subDoc.deepRemove(remaining);
                if (subDoc.size() == 0) {
                    super.remove(key);
                }
            } else if (val == null) {
                super.remove(key);
            }
        }
    }

    private Object getByEmbeddedKey(String embeddedKey) {
        String regex = MessageFormat.format("\\{0}", FIELD_SEPARATOR);

        String[] path = embeddedKey.split(regex);
        if (path.length < 1) {
            return null;
        }

        return this.recursiveGet(get(path[0]), Arrays.copyOfRange(path, 1, path.length));
    }

    @SuppressWarnings("unchecked")
    private Object recursiveGet(Object object, String[] remainingPath) {
        if (object == null) {
            return null;
        }

        if (remainingPath.length == 0) {
            return object;
        }

        if (object instanceof DocumentData) {
            return this.recursiveGet(((DocumentData) object).get(remainingPath[0]),
                Arrays.copyOfRange(remainingPath, 1, remainingPath.length));
        }

        if (object.getClass().isArray()) {
            String accessor = remainingPath[0];
            Object[] array = ObjectUtil.convertToObjectArray(object);

            if (this.isInteger(accessor)) {
                int index = this.asInteger(accessor);
                if (index < 0) {
                    throw new RuntimeException("invalid array index " + index + " to access item inside a document");
                }

                if (index >= array.length)
                    throw new RuntimeException("index " + index +
                        " is not less than the size of the array " + array.length);
                return this.recursiveGet(array[index], Arrays.copyOfRange(remainingPath, 1, remainingPath.length));
            } else {
                return this.decompose(Arrays.asList(array), remainingPath);
            }
        }

        if (object instanceof Iterable) {
            String accessor = remainingPath[0];
            Iterable<Object> iterable = (Iterable<Object>) object;
            List<Object> collection = Iterables.toList(iterable);

            if (this.isInteger(accessor)) {
                int index = this.asInteger(accessor);
                if (index < 0)
                    throw new RuntimeException("invalid collection index " + index + " to access item inside a document");

                if (index >= collection.size())
                    throw new RuntimeException("index " + accessor +
                        " is not less than the size of the list " + collection.size());

                return this.recursiveGet(collection.get(index), Arrays.copyOfRange(remainingPath, 1, remainingPath.length));
            } else {
                return this.decompose(collection, remainingPath);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private List<Object> decompose(List<Object> collection, String[] remainingPath) {
        Set<Object> items = new HashSet<>();

        for (Object item : collection) {
            Object result = this.recursiveGet(item, remainingPath);

            if (result != null) {
                if (result instanceof Iterable) {
                    List<Object> list = Iterables.toList((Iterable<Object>) result);
                    items.addAll(list);
                } else if (result.getClass().isArray()) {
                    List<Object> list = Arrays.asList(ObjectUtil.convertToObjectArray(result));
                    items.addAll(list);
                } else {
                    items.add(result);
                }
            }
        }
        return new ArrayList<>(items);
    }

    private int asInteger(String number) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isEmbedded(String field) {
        return field.contains(FIELD_SEPARATOR);
    }

    @Serial
    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.writeInt(size());
        for (Pair<String, Object> pair : this) {
            stream.writeObject(pair);
        }
    }

    @Serial
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        int size = stream.readInt();
        for (int i = 0; i < size; i++) {
            Pair<String, Object> pair = (Pair<String, Object>) stream.readObject();
            super.put(pair.first(), pair.second());
        }
    }

    @SuppressWarnings("ClassCanBeRecord")
    private final static class MapPairIterator implements Iterator<Pair<String, Object>> {

        private final Iterator<Map.Entry<String, Object>> iterator;

        MapPairIterator(Iterator<Map.Entry<String, Object>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Pair<String, Object> next() {
            Map.Entry<String, Object> next = this.iterator.next();
            return Pair.of(next.getKey(), next.getValue());
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }
    }

}
