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

package org.elasticsearch.index.mapper.geo;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.geoField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 * {GeoFieldMapper}.java
 * nknize, 3/6/15 1:01 PM
 * <p/>
 * Description:
 */
public class GeoFieldMapper extends AbstractFieldMapper<GeoPoint> {
    public static final String CONTENT_TYPE = "geo";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
            FIELD_TYPE.setNumericPrecisionStep(64);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoFieldMapper> {
        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            this.builder = this;
        }

        @Override
        public GeoFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            return new GeoFieldMapper(buildNames(context), fieldType, docValues, indexAnalyzer, searchAnalyzer,
                    similarity, fieldDataSettings, context.indexSettings(), origPathType, multiFieldsBuilder.build(this, context));
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoField(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();

            }
            return builder;
        }
    }

    private final ContentPath.Type pathType;

    public GeoFieldMapper(FieldMapper.Names names, FieldType fieldType, Boolean docValues, NamedAnalyzer indexAnalyzer,
                          NamedAnalyzer searchAnalyzer, SimilarityProvider similarity, @Nullable Settings fieldDataSettings,
                          Settings indexSettings, ContentPath.Type pathType, MultiFields multiFields) {
        super(names, 1f, fieldType, docValues, null, indexAnalyzer, similarity, null, fieldDataSettings, indexSettings, multiFields, null);
        this.pathType = pathType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("geo");
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public GeoPoint value(Object value) {
        throw new UnsupportedOperationException("Geo fields cannot be converted to a value");
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse == null) {
            // in the future this will not be a GeoPoint but an object that will handle shapes
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                        parseGeo(GeoUtils.parseGeoPoint(context.parser(), sparse));
                        token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY) {

                        }
                        parseGeo(sparse.reset(lat, lon));
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                GeoUtils.parseGeoPoint(context.parser(), sparse);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                parseGeo(GeoUtils.parseGeoPoint(context.parser(), sparse));
            }
        }
    }

    private void parseGeo(GeoPoint point) {

        GeoUtils.normalizePoint(point, true, true);

    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parseGeo(sparse.resetFromGeoHash(point));
        } else {
            parseGeo(sparse.resetFromString(point));
        }
    }
}
