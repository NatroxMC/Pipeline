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

package de.natrox.pipeline.document;

import de.natrox.common.container.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentTest {

    private Document document;

    @Test
    public void testCreateDocument() {
        assertTrue(Document.create().isEmpty());
        assertEquals(1, Document.create("Key", "Value").size());
        assertTrue(Document.create(new HashMap<>()).isEmpty());
    }

    @BeforeAll
    public void setup() {
        this.document = Document
            .create("name", "Eric")
            .put("level", 234685)
            .put("address.street", "montana-avenue")
            .put("name.name", "Eric");
    }

    @Test
    public void testGet() {
        assertNull(document.get(""));
        assertEquals(document.get("level"), 234685);
        assertEquals(document.get("name"), "Eric");
        assertEquals(document.get("address.street"), "montana-avenue");
        assertNull(document.get("address.number"));

        assertEquals(document.get("name"), document.get("name"));
        assertEquals(document.get("name.name"), "name");

        assertNotEquals(document.get("name"), "a");
        assertNull(document.get("."));
        assertNull(document.get("level.test"));
    }

    @Test
    public void testPutNull() {
        assertNotNull(document.put("test", null));
        assertNull(document.get("test"));
    }

    @Test
    public void testRemove() {
        Iterator<Pair<String, Object>> iterator = document.iterator();
        assertEquals(document.size(), 4);
        if (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        assertEquals(document.size(), 3);
    }

    @Test
    public void testPut() {
        assertEquals(document.size(), 3);
        document.put("age", 22);

        assertEquals(document.get("age"), 22);
        assertEquals(document.size(), 4);
    }

}
