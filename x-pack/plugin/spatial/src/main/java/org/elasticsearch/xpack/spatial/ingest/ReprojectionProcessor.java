/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.locationtech.proj4j.CoordinateTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ReprojectionProcessor extends AbstractGeometryProcessor {
    public static final ParseField TYPE = new ParseField("reproject");

    private final GeoShapeFieldMapper.CRSHandler crsHandler;
    private final ReprojectionGeometryVisitor reprojector;

    ReprojectionProcessor(String tag, String field, String targetField, boolean ignoreMissing, String srcCRS, String targetCRS,
                          double tolerance, GeometryProcessorFieldType shapeFieldType) {
        super(tag, field, targetField, ignoreMissing, shapeFieldType);
        this.crsHandler = GeoShapeFieldMapper.resolveCRSHandler(srcCRS);
        if (crsHandler == null) {
            throw new IllegalArgumentException("Unable to reproject from: [" + srcCRS + "] to: [" + targetCRS
                + "]: transform not supported");
        }
        CoordinateTransform transformer = (CoordinateTransform)(crsHandler.newTransform(crsHandler.resolveCRS(targetCRS)));
        this.reprojector = new ReprojectionGeometryVisitor(crsHandler, transformer, tolerance);
    }

    @Override
    public Geometry processGeometry(Geometry inGeometry, GeometryFormat geometryFormat) {
        // reproject inGeometry to new geometry
        return inGeometry.visit(reprojector);
    }

    @Override
    public String getType() {
        return TYPE.getPreferredName();
    }

    public static class ReprojectionGeometryVisitor implements GeometryVisitor<Geometry, RuntimeException> {
        private GeoShapeFieldMapper.CRSHandler crsHandler;
        private CoordinateTransform transformer;
        private double tolerance;

        public ReprojectionGeometryVisitor(GeoShapeFieldMapper.CRSHandler crsHandler, CoordinateTransform transform, double tolerance) {
            this.crsHandler = crsHandler;
            this.transformer = transform;
            this.tolerance = tolerance;
        }

        @Override
        public Geometry visit(Circle circle) throws RuntimeException {
            throw new IllegalArgumentException("invalid shape type found [Circle] while reprojecting shape");
        }

        @Override
        public Geometry visit(GeometryCollection<?> collection) throws RuntimeException {
            List<Geometry> reprojectedGeometry = new ArrayList<>(collection.size());
            for (Geometry geometry : collection) {
                reprojectedGeometry.add(geometry.visit(this));
            }
            return new GeometryCollection<>(reprojectedGeometry);
        }

        @Override
        public Line visit(Line line) throws RuntimeException {
            double[][] vertices = reprojectVertices(line.getX(), line.getY(), crsHandler, transformer);
            return new Line(vertices[0], vertices[1]);
        }

        @Override
        public LinearRing visit(LinearRing ring) throws RuntimeException {
            double[][] vertices = reprojectVertices(ring.getX(), ring.getY(), crsHandler, transformer);
            return new LinearRing(vertices[0], vertices[1]);
        }

        @Override
        public MultiLine visit(MultiLine multiLine) throws RuntimeException {
            List<Line> lines = new ArrayList<>(multiLine.size());
            for (Line line : lines) {
                lines.add(visit(line));
            }
            return new MultiLine(lines);
        }

        @Override
        public MultiPoint visit(MultiPoint multiPoint) throws RuntimeException {
            List<Point> points = new ArrayList<>(multiPoint.size());
            for (int i = 0; i < multiPoint.size(); ++i) {
                points.add(visit(multiPoint.get(i)));
            }
            return new MultiPoint(points);
        }

        @Override
        public MultiPolygon visit(MultiPolygon multiPolygon) throws RuntimeException {
            List<Polygon> polys = new ArrayList<>(multiPolygon.size());
            for(int i = 0; i < multiPolygon.size(); ++i) {
                polys.add(visit(multiPolygon.get(i)));
            }
            return new MultiPolygon(polys);
        }

        @Override
        public Point visit(Point point) throws RuntimeException {
            double[] r = crsHandler.reproject(point.getX(), point.getY(), transformer);
            return new Point(r[0], r[1]);
        }

        @Override
        public Polygon visit(Polygon polygon) throws RuntimeException {
            LinearRing shell = visit(polygon.getPolygon());
            List<LinearRing> holes = new ArrayList<>(polygon.getNumberOfHoles());
            for (int i = 0; i < polygon.getNumberOfHoles(); ++i) {
                holes.add(visit(polygon.getHole(i)));
            }
            return new Polygon(shell, holes);
        }

        @Override
        public Rectangle visit(Rectangle rectangle) throws RuntimeException {
            double[] min = crsHandler.reproject(rectangle.getMinX(), rectangle.getMinY(), transformer);
            double[] max = crsHandler.reproject(rectangle.getMaxX(), rectangle.getMaxY(), transformer);
            return new Rectangle(min[0], max[0], max[1], min[1]);
        }
    }

    private static double[][] reprojectVertices(double[] x, double[] y, GeoShapeFieldMapper.CRSHandler crsHandler, CoordinateTransform transform) {
        double[][]result = new double[2][x.length];
        for (int i = 0; i < x.length; ++i) {
            double[] r = crsHandler.reproject(x[i], y[i], transform);
            result[0][i] = r[0];
            result[1][i] = r[1];
        }
        return result;
    }

    public static final class Factory implements Processor.Factory {
        public ReprojectionProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE.getPreferredName(), processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE.getPreferredName(), processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE.getPreferredName(), processorTag, config, "ignore_missing", false);
            String srcCRS = ConfigurationUtils.readStringProperty(TYPE.getPreferredName(), processorTag, config, "source_crs");
            String trgtCRS = ConfigurationUtils.readStringProperty(TYPE.getPreferredName(), processorTag, config, "target_crs");
            double tolerance = ConfigurationUtils.readDoubleProperty(TYPE.getPreferredName(), processorTag, config, "tolerance");
            GeometryProcessorFieldType geometryFieldType = GeometryProcessorFieldType.parse(
                ConfigurationUtils.readStringProperty(TYPE.getPreferredName(), processorTag, config, "shape_type"));
            return new ReprojectionProcessor(processorTag, field, targetField, ignoreMissing, srcCRS, trgtCRS, tolerance, geometryFieldType);
        }
    }
}
