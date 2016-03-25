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

import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.ESTestCase;

/**
 * Basic Tests for {@link GeoDistance}
 */
public class GeoDistanceTests extends ESTestCase {

    public void testDistanceCheck() {
        // Note, is within is an approximation, so, even though 0.52 is outside 50mi, we still get "true"
        GeoRect rect = org.apache.lucene.spatial.util.GeoUtils.circleToBBox(0, 0, DistanceUnit.METERS.convert(50, DistanceUnit.MILES));
        assertTrue(GeoRelationUtils.pointInRectPrecise(0.5, 0.5, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat));
        assertTrue(GeoRelationUtils.pointInRectPrecise(0.52, 0.52, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat));
        assertFalse(GeoRelationUtils.pointInRectPrecise(1, 1, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat));

        rect = org.apache.lucene.spatial.util.GeoUtils.circleToBBox(179, 0, DistanceUnit.METERS.convert(200, DistanceUnit.MILES));
        assertTrue(GeoRelationUtils.pointInRectPrecise(-179, 0, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat));
        assertFalse(GeoRelationUtils.pointInRectPrecise(-178, 0, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat));
    }
}
