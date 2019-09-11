/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.xpack.spatial.ingest.AbstractGeometryProcessor.GeometryProcessorFieldType.GEO_SHAPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class BaseGeometryProcessorTestCase extends ESSingleNodeTestCase {
    protected static final WellKnownText WKT = new WellKnownText(true, new StandardValidator(true));

    public abstract AbstractGeometryProcessor newProcessor(String tag, String field, String targetField, boolean ignoreMissing,
                                                           AbstractGeometryProcessor.GeometryProcessorFieldType shapeFieldType);

    public void testFieldNotFound() throws Exception {
        AbstractGeometryProcessor processor = newProcessor("tag", "field", "field", false, GEO_SHAPE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [field]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        AbstractGeometryProcessor processor = newProcessor("tag", "field", "field", true, GEO_SHAPE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() throws Exception {
        AbstractGeometryProcessor processor = newProcessor("tag", "field", "field", false,  GEO_SHAPE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        AbstractGeometryProcessor processor = newProcessor("tag", "field", "field", true, GEO_SHAPE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testMissingField() {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), Collections.emptyMap());
        AbstractGeometryProcessor processor = newProcessor("tag", "field", "field", false, GEO_SHAPE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public abstract void testJson() throws IOException;
    public abstract void testWKT();
}
