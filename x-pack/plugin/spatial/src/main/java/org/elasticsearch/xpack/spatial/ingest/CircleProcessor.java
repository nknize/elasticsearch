/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.spatial.SpatialUtils;

import java.util.Map;

/**
 *  The circle-processor converts a circle shape definition into a valid regular polygon approximating the circle.
 */
public final class CircleProcessor extends AbstractGeometryProcessor {
    public static final String TYPE = "circle";
    static final int MINIMUM_NUMBER_OF_SIDES = 4;
    static final int MAXIMUM_NUMBER_OF_SIDES = 1000;

    private final double errorDistance;

    CircleProcessor(String tag, String field, String targetField, boolean ignoreMissing, double errorDistance,
                    GeometryProcessorFieldType shapeFieldType) {
        super(tag, field, targetField, ignoreMissing, shapeFieldType);
        this.errorDistance = errorDistance;
    }

    @Override
    public Geometry processGeometry(Geometry inGeometry, GeometryFormat geometryFormat) {
        if (ShapeType.CIRCLE.equals(inGeometry.type())) {
            Circle circle = (Circle) inGeometry;
            int numSides = numSides(circle.getRadiusMeters());
            final Geometry polygonizedCircle;
            switch (shapeFieldType) {
                case GEO_SHAPE:
                    polygonizedCircle = SpatialUtils.createRegularGeoShapePolygon(circle, numSides);
                    break;
                case SHAPE:
                    polygonizedCircle = SpatialUtils.createRegularShapePolygon(circle, numSides);
                    break;
                default:
                    throw new IllegalStateException("invalid shape_type [" + shapeFieldType + "]");
            }
            return polygonizedCircle;
        } else {
            throw new IllegalArgumentException("found [" + inGeometry.type() + "] instead of circle");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    double errorDistance() {
        return errorDistance;
    }

    int numSides(double radiusMeters) {
        int val = (int) Math.ceil(2 * Math.PI / Math.acos(1 - errorDistance / radiusMeters));
        return Math.min(MAXIMUM_NUMBER_OF_SIDES, Math.max(MINIMUM_NUMBER_OF_SIDES, val));
    }

    public static final class Factory implements Processor.Factory {

        public CircleProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            double radiusDistance = Math.abs(ConfigurationUtils.readDoubleProperty(TYPE, processorTag, config, "error_distance"));
            GeometryProcessorFieldType circleFieldType = GeometryProcessorFieldType.parse(
                ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "shape_type"));
            return new CircleProcessor(processorTag, field, targetField, ignoreMissing, radiusDistance, circleFieldType);
        }
    }
}
