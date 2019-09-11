/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractGeometryProcessor extends AbstractProcessor {
    static final GeometryParser PARSER = new GeometryParser(true, true, true);

    protected final String field;
    protected final String targetField;
    protected final boolean ignoreMissing;
    protected final GeometryProcessorFieldType shapeFieldType;

    AbstractGeometryProcessor(String tag, String field, String targetField, boolean ignoreMissing,
                              GeometryProcessorFieldType shapeFieldType) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.shapeFieldType = shapeFieldType;
    }

    String field() {
        return field;
    }

    String targetField() {
        return targetField;
    }

    GeometryProcessorFieldType shapeType() {
        return shapeFieldType;
    }

    public IngestDocument execute(IngestDocument ingestDocument) {
        Object obj = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (obj == null && ignoreMissing) {
            return ingestDocument;
        } else if (obj == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        final Map<String, Object> valueWrapper;
        if (obj instanceof Map || obj instanceof String) {
            valueWrapper = Map.of("shape", obj);
        } else {
            throw new IllegalArgumentException("field [" + field + "] must be a WKT Circle or a GeoJSON Circle value");
        }

        MapXContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, valueWrapper, XContentType.JSON);
        try {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // "shape" field key
            parser.nextToken(); // shape value
            GeometryFormat geometryFormat = PARSER.geometryFormat(parser);
            Geometry inGeometry = geometryFormat.fromXContent(parser);
            Geometry outGeometry = processGeometry(inGeometry, geometryFormat);
            XContentBuilder newValueBuilder = XContentFactory.jsonBuilder().startObject().field("val");
            geometryFormat.toXContent(outGeometry, newValueBuilder, ToXContent.EMPTY_PARAMS);
            newValueBuilder.endObject();
            Map<String, Object> newObj = XContentHelper.convertToMap(
                BytesReference.bytes(newValueBuilder), true, XContentType.JSON).v2();
            ingestDocument.setFieldValue(targetField, newObj.get("val"));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid shape definition", e);
        }
        return ingestDocument;
    }

    public abstract Geometry processGeometry(Geometry inGeometry, GeometryFormat geometryFormat);

    enum GeometryProcessorFieldType {
        SHAPE, GEO_SHAPE;

        public static GeometryProcessorFieldType parse(String value) {
            EnumSet<GeometryProcessorFieldType> validValues = EnumSet.allOf(GeometryProcessorFieldType.class);
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("illegal [shape_type] value [" + value + "]. valid values are " +
                    Arrays.toString(validValues.toArray()));
            }
        }
    }
}
