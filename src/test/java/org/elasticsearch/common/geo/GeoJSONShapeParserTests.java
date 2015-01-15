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

package org.elasticsearch.common.geo;

import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;


/**
 * Tests for {@link GeoJSONShapeParser}
 */
public class GeoJSONShapeParserTests extends ElasticsearchTestCase {

    private final static GeometryFactory GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();
    private final static double PROJECTION_TOLERANCE = 1E-8;

    public void testParse_simplePoint() throws IOException {
        String pointGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Point")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .endObject().string();

        Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson);
    }

    public void testParse_lineString() throws IOException {
        String lineGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "LineString")
                .startArray("coordinates")
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject().string();

        List<Coordinate> lineCoordinates = new ArrayList<>();
        lineCoordinates.add(new Coordinate(100, 0));
        lineCoordinates.add(new Coordinate(101, 1));

        LineString expected = GEOMETRY_FACTORY.createLineString(
                lineCoordinates.toArray(new Coordinate[lineCoordinates.size()]));
        assertGeometryEquals(jtsGeom(expected), lineGeoJson);
    }

    public void testParse_multiLineString() throws IOException {
        String multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiLineString")
                .startArray("coordinates")
                .startArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        MultiLineString expected = GEOMETRY_FACTORY.createMultiLineString(new LineString[]{
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                        new Coordinate(100, 0),
                        new Coordinate(101, 1),
                }),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                        new Coordinate(102, 2),
                        new Coordinate(103, 3),
                }),
        });
        assertGeometryEquals(jtsGeom(expected), multilinesGeoJson);
    }

    public void testParse_circle() throws IOException {
        String multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "circle")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .field("radius", "100m")
                .endObject().string();

        Circle expected = SPATIAL_CONTEXT.makeCircle(100.0, 0.0, 360 * 100 / GeoUtils.EARTH_EQUATOR);
        assertGeometryEquals(expected, multilinesGeoJson);
    }

    public void testParse_envelope() throws IOException {
        // test #1: envelope with expected coordinate order (TopLeft, BottomRight)
        String multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(-50).value(30).endArray()
                .startArray().value(50).value(-30).endArray()
                .endArray()
                .endObject().string();

        Rectangle expected = SPATIAL_CONTEXT.makeRectangle(-50, 50, -30, 30);
        assertGeometryEquals(expected, multilinesGeoJson);

        // test #2: envelope with agnostic coordinate order (TopRight, BottomLeft)
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .endArray()
                .endObject().string();

        expected = SPATIAL_CONTEXT.makeRectangle(-50, 50, -30, 30);
        assertGeometryEquals(expected, multilinesGeoJson);

        // test #3: "envelope" (actually a triangle) with invalid number of coordinates (TopRight, BottomLeft, BottomRight)
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .startArray().value(50).value(30).endArray()
                .startArray().value(-50).value(-30).endArray()
                .startArray().value(50).value(-39).endArray()
                .endArray()
                .endObject().string();
        XContentParser parser = JsonXContent.jsonXContent.createParser(multilinesGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test #4: "envelope" with empty coordinates
        multilinesGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "envelope")
                .startArray("coordinates")
                .endArray()
                .endObject().string();
        parser = JsonXContent.jsonXContent.createParser(multilinesGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
    }

    public void testParse_polygonNoHoles() throws IOException {
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        Coordinate[] shell = new Coordinate[] {
                new Coordinate(100, 0),
                new Coordinate(101, 0),
                new Coordinate(101, 1),
                new Coordinate(100, 1),
                new Coordinate(100, 0)
        };
        Shape expected = createPolygons(shell);
        assertGeometryEquals(expected, polygonGeoJson);
    }

    public void testParse_invalidPoint() throws IOException {
        // test case 1: create an invalid point object with multipoint data format
        String invalidPoint1 = XContentFactory.jsonBuilder().startObject().field("type", "point")
                .startArray("coordinates")
                .startArray().value(-74.011).value(40.753).endArray()
                .endArray()
                .endObject().string();
        XContentParser parser = JsonXContent.jsonXContent.createParser(invalidPoint1);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 2: create an invalid point object with an empty number of coordinates
        String invalidPoint2 = XContentFactory.jsonBuilder().startObject().field("type", "point")
                .startArray("coordinates")
                .endArray()
                .endObject().string();
        parser = JsonXContent.jsonXContent.createParser(invalidPoint2);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
    }

    public void testParse_invalidMultipoint() throws IOException {
        // test case 1: create an invalid multipoint object with single coordinate
        String invalidMultipoint1 = XContentFactory.jsonBuilder().startObject().field("type", "multipoint")
                .startArray("coordinates").value(-74.011).value(40.753).endArray()
                .endObject().string();
        XContentParser parser = JsonXContent.jsonXContent.createParser(invalidMultipoint1);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 2: create an invalid multipoint object with null coordinate
        String invalidMultipoint2 = XContentFactory.jsonBuilder().startObject().field("type", "multipoint")
                .startArray("coordinates")
                .endArray()
                .endObject().string();
        parser = JsonXContent.jsonXContent.createParser(invalidMultipoint2);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 3: create a valid formatted multipoint object with invalid number (0) of coordinates
        String invalidMultipoint3 = XContentFactory.jsonBuilder().startObject().field("type", "multipoint")
                .startArray("coordinates")
                .startArray().endArray()
                .endArray()
                .endObject().string();
        parser = JsonXContent.jsonXContent.createParser(invalidMultipoint3);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
    }

    public void testParse_invalidMultiPolygon() throws IOException {
        // test invalid multipolygon (an "accidental" polygon with inner rings outside outer ring)
        String multiPolygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()//one poly (with two holes)
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .startArray().value(102.0).value(3.0).endArray()
                .startArray().value(102.0).value(2.0).endArray()
                .endArray()
                .startArray()// first hole
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .endArray()
                .startArray()//second hole
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endArray()
                .endObject().string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(multiPolygonGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
    }

    public void testParse_OGCPolygonWithoutHoles() throws IOException {
        // test 1: ccw poly not crossing dateline
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();
        
        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 2: ccw poly crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();
        
        ElasticsearchGeoAssertions.assertMultiPolygon(shape);

        // test 3: cw poly not crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(180.0).value(10.0).endArray()
                .startArray().value(180.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 4: cw poly crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(184.0).value(15.0).endArray()
                .startArray().value(184.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(174.0).value(-10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);
    }

    public void testParse_OGCPolygonWithHoles() throws IOException {
        // test 1: ccw poly not crossing dateline
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 2: ccw poly crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .startArray().value(-180.0).value(-8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);

        // test 3: cw poly not crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(180.0).value(10.0).endArray()
                .startArray().value(179.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(177.0).value(8.0).endArray()
                .startArray().value(179.0).value(10.0).endArray()
                .startArray().value(179.0).value(-8.0).endArray()
                .startArray().value(177.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 4: cw poly crossing dateline
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(183.0).value(10.0).endArray()
                .startArray().value(183.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(183.0).value(10.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(182.0).value(8.0).endArray()
                .startArray().value(180.0).value(-8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);
    }

    public void testParse_invalidPolygon() throws IOException {
        /**
         * The following 3 test cases ensure proper error handling of invalid polygons 
         * per the GeoJSON specification
         */
        // test case 1: create an invalid polygon with only 2 points
        String invalidPoly1 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-74.011).value(40.753).endArray()
                .startArray().value(-75.022).value(41.783).endArray()
                .endArray()
                .endArray()
                .endObject().string();
        XContentParser parser = JsonXContent.jsonXContent.createParser(invalidPoly1);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 2: create an invalid polygon with only 1 point
        String invalidPoly2 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-74.011).value(40.753).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(invalidPoly2);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 3: create an invalid polygon with 0 points
        String invalidPoly3 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(invalidPoly3);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);

        // test case 4: create an invalid polygon with null value points
        String invalidPoly4 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().nullValue().nullValue().endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(invalidPoly4);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchIllegalArgumentException.class);

        // test case 5: create an invalid polygon with 1 invalid LinearRing
        String invalidPoly5 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates")
                .nullValue().nullValue()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(invalidPoly5);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchIllegalArgumentException.class);

        // test case 6: create an invalid polygon with 0 LinearRings
        String invalidPoly6 = XContentFactory.jsonBuilder().startObject().field("type", "polygon")
                .startArray("coordinates").endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(invalidPoly6);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, ElasticsearchParseException.class);
    }

    public void testParse_polygonWithHole() throws IOException {
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        Coordinate[] shell = new Coordinate[] {
                new Coordinate(100, 0),
                new Coordinate(101, 0),
                new Coordinate(101, 1),
                new Coordinate(100, 1)
        };

        Coordinate[][] holes = new Coordinate[][] {{
                new Coordinate(100.2, 0.8),
                new Coordinate(100.2, 0.2),
                new Coordinate(100.8, 0.2),
                new Coordinate(100.8, 0.8)
        }};

        Shape expected = createPolygons(Pair.of(shell, holes));
        assertGeometryEquals(expected, polygonGeoJson);
    }

    public void testParse_selfCrossingPolygon() throws IOException {
        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(-177.0).value(15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertValidException(parser, InvalidShapeException.class);
    }

    public void testParse_multiPoint() throws IOException {
        String multiPointGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPoint")
                .startArray("coordinates")
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .endArray()
                .endObject().string();

        ShapeCollection expected = shapeCollection(
                SPATIAL_CONTEXT.makePoint(100, 0),
                SPATIAL_CONTEXT.makePoint(101, 1.0));
        assertGeometryEquals(expected, multiPointGeoJson);
    }

    public void testParse_multiPolygon() throws IOException {
        // test #1: two polygons; one without hole, one with hole
        String multiPolygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()//first poly (without holes)
                .startArray()
                .startArray().value(102.0).value(2.0).endArray()
                .startArray().value(103.0).value(2.0).endArray()
                .startArray().value(103.0).value(3.0).endArray()
                .startArray().value(102.0).value(3.0).endArray()
                .startArray().value(102.0).value(2.0).endArray()
                .endArray()
                .endArray()
                .startArray()//second poly (with hole)
                .startArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .endArray()
                .startArray()//hole
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endArray()
                .endObject().string();

        Coordinate[] shell1 = new Coordinate[] {
                new Coordinate(102, 3),
                new Coordinate(103, 3),
                new Coordinate(103, 2),
                new Coordinate(102, 2)
        };

        Coordinate[] shell2 = new Coordinate[] {
                new Coordinate(100, 0),
                new Coordinate(101, 0),
                new Coordinate(101, 1),
                new Coordinate(100, 1)
        };

        Coordinate[][] holes = new Coordinate[][] {{
                new Coordinate(100.2, 0.2),
                new Coordinate(100.8, 0.2),
                new Coordinate(100.8, 0.8),
                new Coordinate(100.2, 0.8)
        }};

        Shape expected = createPolygons(new Pair[] {Pair.of(shell1, new Coordinate[][]{}), Pair.of(shell2, holes)});
        assertGeometryEquals(expected, multiPolygonGeoJson);

        // test #2: multipolygon; one polygon with one hole
        // this tests converting the multipolygon from a ShapeCollection type
        // to a simple polygon (jtsGeom)
        multiPolygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "MultiPolygon")
                .startArray("coordinates")
                .startArray()
                .startArray()
                .startArray().value(100.0).value(1.0).endArray()
                .startArray().value(101.0).value(1.0).endArray()
                .startArray().value(101.0).value(0.0).endArray()
                .startArray().value(100.0).value(0.0).endArray()
                .startArray().value(100.0).value(1.0).endArray()
                .endArray()
                .startArray()// hole
                .startArray().value(100.2).value(0.8).endArray()
                .startArray().value(100.2).value(0.2).endArray()
                .startArray().value(100.8).value(0.2).endArray()
                .startArray().value(100.8).value(0.8).endArray()
                .startArray().value(100.2).value(0.8).endArray()
                .endArray()
                .endArray()
                .endArray()
                .endObject().string();

        shell1 = new Coordinate[] {
                new Coordinate(100.0, 1.0),
                new Coordinate(101.0, 1.0),
                new Coordinate(101.0, 0.0),
                new Coordinate(100.0, 0.0)
        };

        holes = new Coordinate[][] {{
                new Coordinate(100.2, 0.8),
                new Coordinate(100.2, 0.2),
                new Coordinate(100.8, 0.2),
                new Coordinate(100.8, 0.8)
        }};

        expected = createPolygons(new Pair[] {Pair.of(shell1, holes)});
        assertGeometryEquals(expected, multiPolygonGeoJson);
    }

    public void testParse_geometryCollection() throws IOException {
        String geometryCollectionGeoJson = XContentFactory.jsonBuilder().startObject()
                .field("type", "GeometryCollection")
                .startArray("geometries")
                    .startObject()
                        .field("type", "LineString")
                        .startArray("coordinates")
                            .startArray().value(100.0).value(0.0).endArray()
                            .startArray().value(101.0).value(1.0).endArray()
                        .endArray()
                    .endObject()
                    .startObject()
                        .field("type", "Point")
                        .startArray("coordinates").value(102.0).value(2.0).endArray()
                    .endObject()
                .endArray()
                .endObject()
                .string();

        Shape[] expected = new Shape[2];
        LineString expectedLineString = GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                new Coordinate(100, 0),
                new Coordinate(101, 1),
        });
        expected[0] = jtsGeom(expectedLineString);
        Point expectedPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(102.0, 2.0));
        expected[1] = new JtsPoint(expectedPoint, SPATIAL_CONTEXT);

        //equals returns true only if geometries are in the same order
        assertGeometryEquals(shapeCollection(expected), geometryCollectionGeoJson);
    }

    public void testThatParserExtractsCorrectTypeAndCoordinatesFromArbitraryJson() throws IOException {
        String pointGeoJson = XContentFactory.jsonBuilder().startObject()
                .startObject("crs")
                    .field("type", "name")
                    .startObject("properties")
                        .field("name", "urn:ogc:def:crs:OGC:1.3:CRS84")
                    .endObject()
                .endObject()
                .field("bbox", "foobar")
                .field("type", "point")
                .field("bubu", "foobar")
                .startArray("coordinates").value(100.0).value(0.0).endArray()
                .startObject("nested").startArray("coordinates").value(200.0).value(0.0).endArray().endObject()
                .startObject("lala").field("type", "NotAPoint").endObject()
                .endObject().string();

        Point expected = GEOMETRY_FACTORY.createPoint(new Coordinate(100.0, 0.0));
        assertGeometryEquals(new JtsPoint(expected, SPATIAL_CONTEXT), pointGeoJson);
    }

    public void testParse_orientationOption() throws IOException {
        // test 1: valid ccw (right handed system) poly not crossing dateline (with 'right' field)
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "right")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 2: valid ccw (right handed system) poly not crossing dateline (with 'ccw' field)
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "ccw")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 3: valid ccw (right handed system) poly not crossing dateline (with 'counterclockwise' field)
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "counterclockwise")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);

        // test 4: valid cw (left handed system) poly crossing dateline (with 'left' field)
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "left")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(180.0).value(-8.0).endArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);

        // test 5: valid cw multipoly (left handed system) poly crossing dateline (with 'cw' field)
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "cw")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(180.0).value(-8.0).endArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);

        // test 6: valid cw multipoly (left handed system) poly crossing dateline (with 'clockwise' field)
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "clockwise")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .startArray().value(178.0).value(8.0).endArray()
                .startArray().value(180.0).value(-8.0).endArray()
                .startArray().value(-178.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .endObject().string();

        parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertMultiPolygon(shape);
    }

    public void testParse_CRSOption() throws IOException {
        // test 1: valid ccw (right handed system) poly not crossing dateline (with 'right' field)
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "right")
                .startArray("coordinates")
                .startArray()
                .startArray().value(176.0).value(15.0).endArray()
                .startArray().value(-177.0).value(10.0).endArray()
                .startArray().value(-177.0).value(-10.0).endArray()
                .startArray().value(176.0).value(-15.0).endArray()
                .startArray().value(172.0).value(0.0).endArray()
                .startArray().value(176.0).value(15.0).endArray()
                .endArray()
                .startArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .startArray().value(174.0).value(10.0).endArray()
                .startArray().value(-172.0).value(-8.0).endArray()
                .startArray().value(-172.0).value(8.0).endArray()
                .endArray()
                .endArray()
                .startObject("crs").field("type", "name")
                .startObject("properties").field("name", "urn:ogc:def:crs:EPSG:1.3:4326")
                .endObject()
                .endObject()
                .endObject()
                .string();

        XContentParser parser = JsonXContent.jsonXContent.createParser(polygonGeoJson);
        parser.nextToken();
        Shape shape = ShapeBuilder.parse(parser).build();

        ElasticsearchGeoAssertions.assertPolygon(shape);
    }

    public void testCRSTransform() throws IOException {
        // test 1: valid ccw (right handed system) poly not crossing dateline (with 'right' field)
        String polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "right")
                .startArray("coordinates")
                .startArray()
                .startArray().value(575025).value(4138885).endArray()
                .startArray().value(573826).value(4138128).endArray()
                .startArray().value(574247).value(4137207).endArray()
                .startArray().value(575357).value(4137134).endArray()
                .startArray().value(576008).value(4137817).endArray()
                .startArray().value(575025).value(4138885).endArray()
                .endArray()
                .startArray()
                .startArray().value(575000).value(4138304).endArray()
                .startArray().value(574779).value(4137994).endArray()
                .startArray().value(575155).value(4138062).endArray()
                .startArray().value(575000).value(4138304).endArray()
                .endArray()
                .endArray()
                .startObject("crs").field("type", "name")
                .startObject("properties").field("name", "urn:ogc:def:crs:EPSG:1.3:3157")
                .endObject()
                .endObject()
                .endObject()
                .string();

        Coordinate[] shell = new Coordinate[] {
                new Coordinate(-122.15241390, 37.39369764),
                new Coordinate(-122.16603372, 37.38697121),
                new Coordinate(-122.16137087, 37.37863671),
                new Coordinate(-122.14884197, 37.37788921),
                new Coordinate(-122.14141949, 37.38399183)
        };

        Coordinate[][] holes = new Coordinate[][] {{
                new Coordinate(-122.15275524, 37.38846320),
                new Coordinate(-122.15528289, 37.38568708),
                new Coordinate(-122.15102904, 37.38626954)
        }};

        Shape expected = createPolygons(Pair.of(shell, holes));
        assertGeometryEquals(expected, polygonGeoJson, PROJECTION_TOLERANCE);

        // test #2: mercator map projection test
        polygonGeoJson = XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
                .field("orientation", "right")
                .startArray("coordinates")
                .startArray()
                .startArray().value(-10848084.37780451).value(3864110.0283834795).endArray()
                .startArray().value(-10859216.326883838).value(3870712.7380463844).endArray()
                .startArray().value(-10892612.174121818).value(3674233.101756766).endArray()
                .startArray().value(-10846971.182896577).value(3545014.813100937).endArray()
                .startArray().value(-10845857.987988645).value(3653465.7266379823).endArray()
                .startArray().value(-10848084.37780451).value(3864110.0283834795).endArray()
                .endArray()
                .endArray()
                .startObject("crs").field("type", "name")
                .startObject("properties").field("name", "urn:ogc:def:crs:EPSG:1.3:3395")
                .endObject()
                .endObject()
                .endObject()
                .string();

        shell = new Coordinate[] {
                new Coordinate(-97.45, 32.94),
                new Coordinate(-97.55, 32.99),
                new Coordinate(-97.85, 31.49),
                new Coordinate(-97.44, 30.49),
                new Coordinate(-97.43, 31.33)
        };

        expected = createPolygons(Pair.of(shell, new Coordinate[][]{}));
        assertGeometryEquals(expected, polygonGeoJson, PROJECTION_TOLERANCE);
    }

    private void assertGeometryEquals(Shape expected, String geoJson) throws IOException {
        assertGeometryEquals(expected, geoJson, 0.0);
    }

    private void assertGeometryEquals(Shape expected, String geoJson, double tolerance) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
        parser.nextToken();
        ElasticsearchGeoAssertions.assertEquals(expected, ShapeBuilder.parse(parser).build(), tolerance);
    }

    private Shape createPolygons(Coordinate[]... shells) {
        Pair<Coordinate[], Coordinate[][]>[] polys = new Pair[shells.length];
        for (int i=0; i<shells.length; ++i) {
            Coordinate[] shell = shells[i];
            polys[i] = Pair.of(shell, new Coordinate[][]{});
        }
        return createPolygons(polys);
    }

    private Shape createPolygons(Pair<Coordinate[], Coordinate[][]>... polygons) {
        Polygon[] polys = new Polygon[polygons.length];
        for (int i = 0; i < polygons.length; ++i) {
            // build shell, ensuring coordinates are closed
            Pair<Coordinate[], Coordinate[][]> poly = polygons[i];
            LinearRing shell = GEOMETRY_FACTORY.createLinearRing(closeRing(poly.getLeft()));
            // build holes
            LinearRing[] holes = new LinearRing[poly.getRight().length];
            for (int j=0; j<holes.length; ++j) {
                holes[j] = GEOMETRY_FACTORY.createLinearRing(closeRing(poly.getRight()[j]));
            }
            polys[i] = GEOMETRY_FACTORY.createPolygon(shell, holes);
        }

       return (polys.length > 1) ? shapeCollection(polys) : jtsGeom(polys[0]);
    }

    private Coordinate[] closeRing(Coordinate[] ring) {
        int length = ring.length;
        Coordinate[] newRing = ring;
        if (!ring[0].equals(ring[length-1])) {
            newRing = new Coordinate[length+1];
            System.arraycopy(ring, 0, newRing, 0, length);
            newRing[length] = newRing[0];
        }
        return newRing;
    }

    private ShapeCollection<Shape> shapeCollection(Shape... shapes) {
        return new ShapeCollection<>(Arrays.asList(shapes), SPATIAL_CONTEXT);
    }

    private ShapeCollection<Shape> shapeCollection(Geometry... geoms) {
        List<Shape> shapes = new ArrayList<>(geoms.length);
        for (Geometry geom : geoms) {
            shapes.add(jtsGeom(geom));
        }
        return new ShapeCollection<>(shapes, SPATIAL_CONTEXT);
    }

    private JtsGeometry jtsGeom(Geometry geom) {
        return new JtsGeometry(geom, SPATIAL_CONTEXT, false, false);
    }
}