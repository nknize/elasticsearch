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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.locationtech.proj4j.CoordinateTransform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.spatial.ingest.AbstractGeometryProcessor.GeometryProcessorFieldType.GEO_SHAPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ReprojectionProcessorTests extends BaseGeometryProcessorTestCase {
    private static String srcCRS = "EPSG:23031"; // ??
    private static String trgtCRS = "EPSG:4326"; // WGS84
    private static double tolerance = 0.1;

    private static GeoShapeFieldMapper.CRSHandler crsHandler;
    private static CoordinateTransform transformer;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, SpatialPlugin.class, XPackPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        crsHandler = GeoShapeFieldMapper.resolveCRSHandler(srcCRS);
        transformer = (CoordinateTransform)(crsHandler.newTransform(crsHandler.resolveCRS(trgtCRS)));
    }

    @Override
    public ReprojectionProcessor newProcessor(String tag, String field, String targetField, boolean ignoreMissing,
                                              AbstractGeometryProcessor.GeometryProcessorFieldType shapeFieldType) {
        return new ReprojectionProcessor(tag, field, targetField, ignoreMissing, srcCRS, trgtCRS, tolerance, shapeFieldType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testJson() throws IOException {
        LinearRing shell = new LinearRing(
            new double[] {556878.9016076007, 566878.9016076007, 566878.9016076007, 556878.9016076007},
            new double[] {5682145.166264554, 5782145.166264554, 5682145.166264554, 5682145.166264554});
        Polygon polygon = new Polygon(shell);
        HashMap<String, Object> map = new HashMap<>();
        HashMap<String, Object> geometryMap = new HashMap<>();
        geometryMap.put("type", "Polygon");
        List<List<Double>> coordinates = new ArrayList<>(shell.length());
        for (int i = 0; i < shell.length(); ++i) {
            coordinates.add(List.of(shell.getX(i), shell.getY(i)));
        }
        List<List<List<Double>>> polyAsList = new ArrayList<>(1);
        polyAsList.add(coordinates);
        geometryMap.put("coordinates", polyAsList);
        map.put("field", geometryMap);
        ReprojectionProcessor.ReprojectionGeometryVisitor visitor =
            new ReprojectionProcessor.ReprojectionGeometryVisitor(crsHandler, transformer, tolerance);
        Geometry expectedPoly = polygon.visit(visitor);
        assertThat(expectedPoly, instanceOf(Polygon.class));
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        ReprojectionProcessor processor = newProcessor("tag", "field", "wgs84", false, GEO_SHAPE);
        processor.execute(ingestDocument);
        Map<String, Object> polyMap = ingestDocument.getFieldValue("wgs84", Map.class);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(expectedPoly, builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> expected = XContentHelper.convertToMap(BytesReference.bytes(builder),
            true, XContentType.JSON);
        assertThat(polyMap, equalTo(expected.v2()));
    }

    @Override
    public void testWKT() {
        LinearRing shell = new LinearRing(
            new double[] {556878.9016076007, 566878.9016076007, 566878.9016076007, 556878.9016076007},
            new double[] {5682145.166264554, 5782145.166264554, 5682145.166264554, 5682145.166264554});
        Polygon polygon = new Polygon(shell);
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", WKT.toWKT(polygon));
        ReprojectionProcessor.ReprojectionGeometryVisitor visitor =
            new ReprojectionProcessor.ReprojectionGeometryVisitor(crsHandler, transformer, tolerance);
        Geometry expectedPoly = polygon.visit(visitor);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        ReprojectionProcessor processor = newProcessor("tag", "field", "wgs84", false, GEO_SHAPE);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("wgs84", String.class);
        assertThat(polyString, equalTo(WKT.toWKT(expectedPoly)));
    }
}
