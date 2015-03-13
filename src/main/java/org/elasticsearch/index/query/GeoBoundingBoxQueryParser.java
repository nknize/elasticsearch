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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.GeoPointQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * {GeoBoundingBoxQueryParser}.java
 * nknize, 3/2/15 7:55 AM
 * <p/>
 * Description:
 */
public class GeoBoundingBoxQueryParser implements QueryParser {
    public static final String NAME = "bounding_box";

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        String fieldName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        float boost = 1f;
        String queryName = null;

        Pair<GeoPoint, GeoPoint> bbox = Pair.of(new GeoPoint(), new GeoPoint());

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT ) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (GeoBoundingBoxFilterParser.FIELD.equals(currentFieldName)) {
                            fieldName = parser.text();
                        } else {
                            GeoBoundingBoxFilterParser.parseCoordinates(currentFieldName, parser, bbox);
                        }
                    } else {
                        throw new ElasticsearchParseException("fieldname expected but [" + token + "] found");
                    }
                }
            } else if (token.isValue()) {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bounding_box] query does not support [" + currentFieldName + "]");
                }
            }
        }

        GeoPoint topLeft = bbox.getLeft();
        GeoPoint bottomRight = bbox.getRight();

        Query query = GeoPointQuery.newBoundingBoxQuery(fieldName, topLeft.lon(), bottomRight.lat(), bottomRight.lon(),
                topLeft.lat());

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

        return query;
    }
}
