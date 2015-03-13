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

package org.elasticsearch.index.query;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * {GeoBoundingBoxQueryBuilder}.java
 * nknize, 3/2/15 7:37 AM
 * <p/>
 * Description:
 */
public class GeoBoundingBoxQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<GeoBoundingBoxQueryBuilder> {
    private final String name;
    private final GeoPoint topLeft;
    private final GeoPoint bottomRight;

    private float boost = -1;
    private String queryName;

    public GeoBoundingBoxQueryBuilder(String name, GeoPoint topLeft, GeoPoint bottomRight) {
        this.name = name;
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    @Override
    public GeoBoundingBoxQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public GeoBoundingBoxQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoBoundingBoxQueryParser.NAME);

        builder.startObject(name);

        if (topLeft != null) {
            builder.field("top_left", topLeft);
        }
        if (bottomRight != null) {
            builder.field("bottom_right", bottomRight);
        }

        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
        builder.endObject();
    }
}
