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

package org.elasticsearch.common.geo.builders;

import com.google.common.collect.Sets;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.osgeo.proj4j.CRSFactory;
import org.osgeo.proj4j.CoordinateReferenceSystem;
import org.osgeo.proj4j.CoordinateTransform;
import org.osgeo.proj4j.CoordinateTransformFactory;
import org.osgeo.proj4j.ProjCoordinate;

import java.io.IOException;
import java.util.*;

/**
 * Basic class for building GeoJSON shapes like Polygons, Linestrings, etc 
 */
public abstract class ShapeBuilder implements ToXContent {

    protected static final ESLogger LOGGER = ESLoggerFactory.getLogger(ShapeBuilder.class.getName());

    private static final boolean DEBUG;
    static {
        // if asserts are enabled we run the debug statements even if they are not logged
        // to prevent exceptions only present if debug enabled
        boolean debug = false;
        assert debug = true;
        DEBUG = debug;
    }

    public static final double DATELINE = 180;
    // TODO how might we use JtsSpatialContextFactory to configure the context (esp. for non-geo)?
    public static final JtsSpatialContext SPATIAL_CONTEXT = JtsSpatialContext.GEO;
    public static final GeometryFactory FACTORY = SPATIAL_CONTEXT.getGeometryFactory();
    public static final CRSFactory CRS_FACTORY = new CRSFactory();
    protected static final CoordinateTransformFactory TRANSFORM_FACTORY = new CoordinateTransformFactory();

    /** base WGS84 projection parameters */
    protected static final String WGS84_PARAM = "+title=long/lat:WGS84 +proj=longlat +datum=WGS84 +units=degrees";
    public static final CoordinateReferenceSystem WGS84 = CRS.CRS84.crs;

    protected static final Set<String> PROJ_AUTHORITIES = Sets.newHashSet("EPSG", "ESRI", "NAD27", "NAD83", "WORLD", "OGC");

    /** We're expecting some geometries might cross the dateline. */
    protected final boolean wrapdateline = SPATIAL_CONTEXT.isGeo();

    /** It's possible that some geometries in a MULTI* shape might overlap. With the possible exception of GeometryCollection,
     * this normally isn't allowed.
     */
    protected final boolean multiPolygonMayOverlap = false;
    /** @see com.spatial4j.core.shape.jts.JtsGeometry#validate() */
    protected final boolean autoValidateJtsGeometry = true;
    /** @see com.spatial4j.core.shape.jts.JtsGeometry#index() */
    protected final boolean autoIndexJtsGeometry = true;//may want to turn off once SpatialStrategy impls do it.

    protected Orientation orientation = Orientation.RIGHT;
    protected CoordinateReferenceSystem crs = WGS84;

    protected ShapeBuilder() {

    }

    protected ShapeBuilder(Orientation orientation, CoordinateReferenceSystem crs) {
        this.orientation = orientation;
        this.crs = crs;
    }

    protected static Coordinate coordinate(Coordinate coord, CoordinateReferenceSystem srcCRS, CoordinateReferenceSystem tgtCRS) {
        ProjCoordinate pout = reproject(coord.x, coord.y, srcCRS, tgtCRS);
        coord.setOrdinate(Coordinate.X, pout.x);
        coord.setOrdinate(Coordinate.Y, pout.y);
        coord.setOrdinate(Coordinate.Z, pout.z);
        return coord;
    }

    protected static Coordinate coordinate(double longitude, double latitude, CoordinateReferenceSystem crs) {
        // project from given crs to WGS84
        if (crs != WGS84) {
            ProjCoordinate pout = reproject(longitude, latitude, crs, WGS84);
            return new Coordinate(pout.x, pout.y);
        }
        return new Coordinate(longitude, latitude);
    }

    protected JtsGeometry jtsGeometry(Geometry geom) {
        //dateline180Check is false because ElasticSearch does it's own dateline wrapping
        JtsGeometry jtsGeometry = new JtsGeometry(geom, SPATIAL_CONTEXT, false, multiPolygonMayOverlap);
        if (autoValidateJtsGeometry)
            jtsGeometry.validate();
        if (autoIndexJtsGeometry)
            jtsGeometry.index();
        return jtsGeometry;
    }

    /**
     * Create a new point
     *
     * @param longitude longitude of the point
     * @param latitude latitude of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(double longitude, double latitude) {
        return newPoint(new Coordinate(longitude, latitude), WGS84);
    }

    /**
     * Create a new point
     * 
     * @param longitude longitude of the point
     * @param latitude latitude of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(double longitude, double latitude, CoordinateReferenceSystem crs) {
        return newPoint(new Coordinate(longitude, latitude), crs);
    }

    /**
     * Create a new {@link PointBuilder} from a {@link Coordinate}
     * @param coordinate coordinate defining the position of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(Coordinate coordinate, CoordinateReferenceSystem crs) {
        return new PointBuilder().coordinate(coordinate, crs);
    }

    /**
     * Create a new set of points
     * @return new {@link MultiPointBuilder}
     */
    public static MultiPointBuilder newMultiPoint() {
        return new MultiPointBuilder(WGS84);
    }

    /**
     * Create a new set of points
     * @return new {@link MultiPointBuilder}
     */
    public static MultiPointBuilder newMultiPoint(CoordinateReferenceSystem crs) {
        return new MultiPointBuilder(crs);
    }

    /**
     * Create a new lineString
     * @return a new {@link LineStringBuilder}
     */
    public static LineStringBuilder newLineString() {
        return new LineStringBuilder(WGS84);
    }

    /**
     * Create a new lineString
     * @return a new {@link LineStringBuilder}
     */
    public static LineStringBuilder newLineString(CoordinateReferenceSystem crs) {
        return new LineStringBuilder(crs);
    }

    /**
     * Create a new Collection of lineStrings
     * @return a new {@link MultiLineStringBuilder}
     */
    public static MultiLineStringBuilder newMultiLinestring() {
        return new MultiLineStringBuilder(WGS84);
    }

    /**
     * Create a new Collection of lineStrings
     * @return a new {@link MultiLineStringBuilder}
     */
    public static MultiLineStringBuilder newMultiLinestring(CoordinateReferenceSystem crs) {
        return new MultiLineStringBuilder(crs);
    }

    /**
     * Create a new Polygon
     * @return a new {@link PointBuilder}
     */
    public static PolygonBuilder newPolygon() {
        return new PolygonBuilder();
    }

    /**
     * Create a new Polygon
     * @return a new {@link PointBuilder}
     */
    public static PolygonBuilder newPolygon(Orientation orientation, CoordinateReferenceSystem crs) {
        return new PolygonBuilder(orientation, crs);
    }

    /**
     * Create a new Collection of polygons
     * @return a new {@link MultiPolygonBuilder}
     */
    public static MultiPolygonBuilder newMultiPolygon() {
        return new MultiPolygonBuilder();
    }

    /**
     * Create a new Collection of polygons
     * @return a new {@link MultiPolygonBuilder}
     */
    public static MultiPolygonBuilder newMultiPolygon(Orientation orientation, CoordinateReferenceSystem crs) {
        return new MultiPolygonBuilder(orientation, crs);
    }

    /**
     * Create a new GeometryCollection
     * @return a new {@link GeometryCollectionBuilder}
     */
    public static GeometryCollectionBuilder newGeometryCollection() {
        return new GeometryCollectionBuilder();
    }

    /**
     * Create a new GeometryCollection
     * @return a new {@link GeometryCollectionBuilder}
     */
    public static GeometryCollectionBuilder newGeometryCollection(Orientation orientation, CoordinateReferenceSystem crs) {
        return new GeometryCollectionBuilder(orientation, crs);
    }

    /**
     * create a new Circle
     * @return a new {@link CircleBuilder}
     */
    public static CircleBuilder newCircleBuilder() {
        return new CircleBuilder();
    }

    /**
     * create a new rectangle
     * @return a new {@link EnvelopeBuilder}
     */
    public static EnvelopeBuilder newEnvelope() { return new EnvelopeBuilder(); }

    /**
     * create a new rectangle
     * @return a new {@link EnvelopeBuilder}
     */
    public static EnvelopeBuilder newEnvelope(Orientation orientation, CoordinateReferenceSystem crs) {
        return new EnvelopeBuilder(orientation, crs);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder xcontent = JsonXContent.contentBuilder();
            return toXContent(xcontent, EMPTY_PARAMS).prettyPrint().string();
        } catch (IOException e) {
            return super.toString();
        }
    }

    /**
     * Create a new Shape from this builder. Since calling this method could change the
     * defined shape. (by inserting new coordinates or change the position of points)
     * the builder looses its validity. So this method should only be called once on a builder  
     * @return new {@link Shape} defined by the builder
     */
    public abstract Shape build();

    /**
     * Recursive method which parses the arrays of coordinates used to define
     * Shapes
     * 
     * @param parser
     *            Parser that will be read from
     * @return CoordinateNode representing the start of the coordinate tree
     * @throws IOException
     *             Thrown if an error occurs while reading from the
     *             XContentParser
     */
    private static CoordinateNode parseCoordinates(XContentParser parser, CoordinateReferenceSystem crs) throws IOException {
        XContentParser.Token token = parser.nextToken();

        // Base cases
        if (token != XContentParser.Token.START_ARRAY && 
                token != XContentParser.Token.END_ARRAY && 
                token != XContentParser.Token.VALUE_NULL) {
            double lon = parser.doubleValue();
            token = parser.nextToken();
            double lat = parser.doubleValue();
            token = parser.nextToken();
            return new CoordinateNode(new Coordinate(lon, lat));
        } else if (token == XContentParser.Token.VALUE_NULL) {
            throw new ElasticsearchIllegalArgumentException("coordinates cannot contain NULL values)");
        }

        List<CoordinateNode> nodes = new ArrayList<>();
        while (token != XContentParser.Token.END_ARRAY) {
            nodes.add(parseCoordinates(parser, crs));
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code><null</code>
     * @throws IOException if the input could not be read
     */
    public static ShapeBuilder parse(XContentParser parser) throws IOException {
        return GeoShapeType.parse(parser, null);
    }

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @param geoDocMapper document field mapper reference required for spatial parameters relevant
     *                     to the shape construction process (e.g., orientation)
     *                     todo: refactor to place build specific parameters in the SpatialContext
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code><null</code>
     * @throws IOException if the input could not be read
     */
    public static ShapeBuilder parse(XContentParser parser, GeoShapeFieldMapper geoDocMapper) throws IOException {
        return GeoShapeType.parse(parser, geoDocMapper);
    }

    protected static XContentBuilder toXContent(XContentBuilder builder, Coordinate coordinate) throws IOException {
        return builder.startArray().value(coordinate.x).value(coordinate.y).endArray();
    }

    public static CoordinateReferenceSystem createCRSfromName(String crsSpec) {
        CoordinateReferenceSystem crs = null;
        // test if name is a PROJ4 spec
        if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0) {
            crs = CRS_FACTORY.createFromParameters("Anon", crsSpec);
        }
        else {
            crs = CRS_FACTORY.createFromName(crsSpec);
        }
        return crs;
    }

    public static CoordinateReferenceSystem createCRSfromLink(String crsStr) {
        // TODO support projection definitions by way of links
        return null;
    }

    public static Orientation orientationFromString(String orientation) {
        orientation = orientation.toLowerCase(Locale.ROOT);
        switch (orientation) {
            case "right":
            case "counterclockwise":
            case "ccw":
                return Orientation.RIGHT;
            case "left":
            case "clockwise":
            case "cw":
                return Orientation.LEFT;
            default:
                throw new IllegalArgumentException("Unknown orientation [" + orientation + "]");
        }
    }

    protected static ProjCoordinate reproject(double lon, double lat, CoordinateReferenceSystem srcCRS,
                                              CoordinateReferenceSystem tgtCRS) {
        // reproject from given crs to WGS84
        CoordinateTransform trans = TRANSFORM_FACTORY.createTransform(srcCRS, tgtCRS);
        ProjCoordinate pout = new ProjCoordinate();
        trans.transform(new ProjCoordinate(lon, lat), pout);
        return pout;
    }

    protected static Coordinate shift(Coordinate coordinate, double dateline) {
        if (dateline == 0) {
            return coordinate;
        } else {
            return new Coordinate(-2 * dateline + coordinate.x, coordinate.y);
        }
    }

    /**
     * get the shapes type
     * @return type of the shape
     */
    public abstract GeoShapeType type();

    /**
     * Calculate the intersection of a line segment and a vertical dateline.
     * 
     * @param p1
     *            start-point of the line segment
     * @param p2
     *            end-point of the line segment
     * @param dateline
     *            x-coordinate of the vertical dateline
     * @return position of the intersection in the open range (0..1] if the line
     *         segment intersects with the line segment. Otherwise this method
     *         returns {@link Double#NaN}
     */
    protected static final double intersection(Coordinate p1, Coordinate p2, double dateline) {
        if (p1.x == p2.x && p1.x != dateline) {
            return Double.NaN;
        } else if (p1.x == p2.x && p1.x == dateline) {
            return 1.0;
        } else {
            final double t = (dateline - p1.x) / (p2.x - p1.x);
            if (t > 1 || t <= 0) {
                return Double.NaN;
            } else {
                return t;
            }
        }
    }

    /**
     * Calculate all intersections of line segments and a vertical line. The
     * Array of edges will be ordered asc by the y-coordinate of the
     * intersections of edges.
     * 
     * @param dateline
     *            x-coordinate of the dateline
     * @param edges
     *            set of edges that may intersect with the dateline
     * @return number of intersecting edges
     */
    protected static int intersections(double dateline, Edge[] edges) {
        int numIntersections = 0;
        assert !Double.isNaN(dateline);
        for (int i = 0; i < edges.length; i++) {
            Coordinate p1 = edges[i].coordinate;
            Coordinate p2 = edges[i].next.coordinate;
            assert !Double.isNaN(p2.x) && !Double.isNaN(p1.x);  
            edges[i].intersect = Edge.MAX_COORDINATE;

            double position = intersection(p1, p2, dateline);
            if (!Double.isNaN(position)) {
                edges[i].intersection(position);
                numIntersections++;
            }
        }
        Arrays.sort(edges, INTERSECTION_ORDER);
        return numIntersections;
    }

    /**
     * Node used to represent a tree of coordinates.
     * <p/>
     * Can either be a leaf node consisting of a Coordinate, or a parent with
     * children
     */
    protected static class CoordinateNode implements ToXContent {

        protected final Coordinate coordinate;
        protected final List<CoordinateNode> children;
        protected CoordinateReferenceSystem crs = null;

        /**
         * Creates a new leaf CoordinateNode
         * 
         * @param coordinate
         *            Coordinate for the Node
         */
        protected CoordinateNode(Coordinate coordinate) {
            this.coordinate = coordinate;
            this.children = null;
        }

        /**
         * Creates a new parent CoordinateNode
         * 
         * @param children
         *            Children of the Node
         */
        protected CoordinateNode(List<CoordinateNode> children) {
            this.children = children;
            this.coordinate = null;
        }

        protected boolean isEmpty() {
            return (coordinate == null && (children == null || children.isEmpty()));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (children == null) {
                builder.startArray().value(coordinate.x).value(coordinate.y).endArray();
            } else {
                builder.startArray();
                for (CoordinateNode child : children) {
                    child.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }
    }

    /**
     * This helper class implements a linked list for {@link Coordinate}. It contains
     * fields for a dateline intersection and component id 
     */
    protected static final class Edge {
        Coordinate coordinate; // coordinate of the start point
        Edge next; // next segment
        Coordinate intersect; // potential intersection with dateline
        int component = -1; // id of the component this edge belongs to
        public static final Coordinate MAX_COORDINATE = new Coordinate(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

        protected Edge(Coordinate coordinate, Edge next, Coordinate intersection) {
            this.coordinate = coordinate;
            this.next = next;
            this.intersect = intersection;
            if (next != null) {
                this.component = next.component;
            }
        }

        protected Edge(Coordinate coordinate, Edge next) {
            this(coordinate, next, Edge.MAX_COORDINATE);
        }

        private static final int top(Coordinate[] points, int offset, int length) {
            int top = 0; // we start at 1 here since top points to 0
            for (int i = 1; i < length; i++) {
                if (points[offset + i].y < points[offset + top].y) {
                    top = i;
                } else if (points[offset + i].y == points[offset + top].y) {
                    if (points[offset + i].x < points[offset + top].x) {
                        top = i;
                    }
                }
            }
            return top;
        }

        private static final Pair range(Coordinate[] points, int offset, int length) {
            double minX = points[0].x;
            double maxX = points[0].x;
            double minY = points[0].y;
            double maxY = points[0].y;
            // compute the bounding coordinates (@todo: cleanup brute force)
            for (int i = 1; i < length; ++i) {
                if (points[offset + i].x < minX) {
                    minX = points[offset + i].x;
                }
                if (points[offset + i].x > maxX) {
                    maxX = points[offset + i].x;
                }
                if (points[offset + i].y < minY) {
                    minY = points[offset + i].y;
                }
                if (points[offset + i].y > maxY) {
                    maxY = points[offset + i].y;
                }
            }
            return Pair.of(Pair.of(minX, maxX), Pair.of(minY, maxY));
        }

        /**
         * Concatenate a set of points to a polygon
         * 
         * @param component
         *            component id of the polygon
         * @param direction
         *            direction of the ring
         * @param points
         *            list of points to concatenate
         * @param pointOffset
         *            index of the first point
         * @param edges
         *            Array of edges to write the result to
         * @param edgeOffset
         *            index of the first edge in the result
         * @param length
         *            number of points to use
         * @return the edges creates
         */
        private static Edge[] concat(int component, boolean direction, Coordinate[] points, final int pointOffset, Edge[] edges, final int edgeOffset,
                int length) {
            assert edges.length >= length+edgeOffset;
            assert points.length >= length+pointOffset;
            edges[edgeOffset] = new Edge(points[pointOffset], null);
            for (int i = 1; i < length; i++) {
                if (direction) {
                    edges[edgeOffset + i] = new Edge(points[pointOffset + i], edges[edgeOffset + i - 1]);
                    edges[edgeOffset + i].component = component;
                } else {
                    edges[edgeOffset + i - 1].next = edges[edgeOffset + i] = new Edge(points[pointOffset + i], null);
                    edges[edgeOffset + i - 1].component = component;
                }
            }

            if (direction) {
                edges[edgeOffset].next = edges[edgeOffset + length - 1];
                edges[edgeOffset].component = component;
            } else {
                edges[edgeOffset + length - 1].next = edges[edgeOffset];
                edges[edgeOffset + length - 1].component = component;
            }

            return edges;
        }

        /**
         * Create a connected list of a list of coordinates
         * 
         * @param points
         *            array of point
         * @param offset
         *            index of the first point
         * @param length
         *            number of points
         * @return Array of edges
         */
        protected static Edge[] ring(int component, boolean direction, boolean handedness, BaseLineStringBuilder<?> shell,
                                     Coordinate[] points, int offset, Edge[] edges, int toffset, int length) {
            // calculate the direction of the points:
            // find the point a the top of the set and check its
            // neighbors orientation. So direction is equivalent
            // to clockwise/counterclockwise
            final int top = top(points, offset, length);
            final int prev = (offset + ((top + length - 1) % length));
            final int next = (offset + ((top + 1) % length));
            boolean orientation = points[offset + prev].x > points[offset + next].x;

            // OGC requires shell as ccw (Right-Handedness) and holes as cw (Left-Handedness) 
            // since GeoJSON doesn't specify (and doesn't need to) GEO core will assume OGC standards
            // thus if orientation is computed as cw, the logic will translate points across dateline
            // and convert to a right handed system

            // compute the bounding box and calculate range
            Pair<Pair, Pair> range = range(points, offset, length);
            final double rng = (Double)range.getLeft().getRight() - (Double)range.getLeft().getLeft();
            // translate the points if the following is true
            //   1.  shell orientation is cw and range is greater than a hemisphere (180 degrees) but not spanning 2 hemispheres 
            //       (translation would result in a collapsed poly)
            //   2.  the shell of the candidate hole has been translated (to preserve the coordinate system)
            boolean incorrectOrientation = component == 0 && handedness != orientation;
            if ( (incorrectOrientation && (rng > DATELINE && rng != 2*DATELINE)) || (shell.translated && component != 0)) {
                translate(points);
                // flip the translation bit if the shell is being translated
                if (component == 0) {
                    shell.translated = true;
                }
                // correct the orientation post translation (ccw for shell, cw for holes)
                if (component == 0 || (component != 0 && handedness == orientation)) {
                    orientation = !orientation;
                }
            }
            return concat(component, direction ^ orientation, points, offset, edges, toffset, length);
        }

        /**
         * Transforms coordinates in the eastern hemisphere (-180:0) to a (180:360) range 
         * @param points
         */
        protected static void translate(Coordinate[] points) {
            for (Coordinate c : points) {
                if (c.x < 0) {
                    c.x += 2*DATELINE;
                }
            }
        }

        /**
         * Set the intersection of this line segment to the given position
         * 
         * @param position
         *            position of the intersection [0..1]
         * @return the {@link Coordinate} of the intersection
         */
        protected Coordinate intersection(double position) {
            return intersect = position(coordinate, next.coordinate, position);
        }

        public static Coordinate position(Coordinate p1, Coordinate p2, double position) {
            if (position == 0) {
                return p1;
            } else if (position == 1) {
                return p2;
            } else {
                final double x = p1.x + position * (p2.x - p1.x);
                final double y = p1.y + position * (p2.y - p1.y);
                return new Coordinate(x, y);
            }
        }

        @Override
        public String toString() {
            return "Edge[Component=" + component + "; start=" + coordinate + " " + "; intersection=" + intersect + "]";
        }
    }

    protected static final IntersectionOrder INTERSECTION_ORDER = new IntersectionOrder();

    private static final class IntersectionOrder implements Comparator<Edge> {
        @Override
        public int compare(Edge o1, Edge o2) {
            return Double.compare(o1.intersect.y, o2.intersect.y);
        }

    }

    public static final class CRSBuilder {
        public static CoordinateReferenceSystem parseCRS(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("crs must be an object");
            }

            XContentParser.Token token;
            String crsType = null;
            CoordinateReferenceSystem crs = WGS84;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if (FIELD_TYPE.equals(fieldName)) {
                        parser.nextToken();
                        crsType = parser.text();
                        if (!crsType.equals("name") && !crsType.equals("link")) {
                            throw new ElasticsearchParseException("no crs type [" + crsType + "] Expected one of 'name' or 'link'");
                        }
                    } else if (FIELD_PROPERTIES.equals(fieldName)) {
                        if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
                            token = parser.nextToken();
                        } else {
                            throw new ElasticsearchParseException("expected crs properties object.");
                        }
                        if (crsType == null) {
                        } else if (crsType.equals("name")) {
                            parser.nextToken();
                            String name = convertToProj4Name(parser.text());
                            if (name.substring(0,3).equals("OGC")) {
                                crs = CRS.forName(name.substring(4));
                            } else {
                                crs = CRS_FACTORY.createFromName(convertToProj4Name(parser.text()));
                            }
                            // throw away garbage until reach end of 'properties' object
                            while ( (parser.nextToken()) != XContentParser.Token.END_OBJECT );
                        } else if (crsType.equals("link")) {
                            throw new ElasticsearchParseException("crs 'link' type not yet supported");
                        }
                    }
                }
            }

            return crs;
        }

        public static String convertToProj4Name(String ogcURN) {
            // split on ':'
            String[] ogcParts = ogcURN.split(":");

            if (ogcParts.length != 7) {
                throw new ElasticsearchParseException("invalid OGC URN [" + ogcURN + "] must be in the format: " +
                        "urn:ogc:def:objectType:authority:version:code'");
            }

            // enforce crs objectType definitions
            if (!ogcParts[3].equals("crs")) {
                throw new ElasticsearchParseException("can only handle 'crs' OGC objectType [" + ogcParts[3] + "] provided");
            }

            // enforce EPSG, ESRI, NAD27, NAD83, and WORLD authority
            if (!PROJ_AUTHORITIES.contains(ogcParts[4])) {
                throw new ElasticsearchParseException("invalid projection authority [" + ogcParts[4] +
                        "] must be one of " + PROJ_AUTHORITIES);
            }

            return ogcParts[4] + ":" + ogcParts[6];
        }

        public static void doXContentBody(XContentBuilder builder, CoordinateReferenceSystem crs) throws IOException {

            builder.startObject("crs")
            .field("type", "name")
            .startObject("properties")
            .field("name", "urn:ogc:def:crs:" + crs.getName().replace(":", ":1.3:"))
            .endObject()
            .endObject();

        }
    }

    protected static enum CRS {
        // TODO MAPCS("CRS1", "+title=s/l:map +proj=cartesian +datum="),
        CRS84("CRS84", "+title=long/lat:WGS84 +proj=longlat +datum=WGS84 +units=degrees"),
        CRS83("CRS83", "+title=long/lat:NAD83 +proj=longlat +datum=NAD83 +units=degrees"),
        CRS27("CRS27", "+title=long/lat:NAD27 +proj=longlat +datum=NAD27 +units=degrees");
        // TODO NAVD88("CRS88", ""),
        // TODO UTM("AUTO42001", "+title=long/lat:WGS84 +proj=longlat +datum=WGS84 +units=degrees"),
        // TODO ATM("AUTO42002", ""),
        // TODO ORTHO("AUTO42003", ""),
        // TODO EQUIRECT("AUTO42004", ""),
        // TODO MOLL("AUTO42005", "");

        protected String name;
        protected CoordinateReferenceSystem crs;

        private CRS(String name, String params) {
            this.name = name;
            crs = CRS_FACTORY.createFromParameters(name, params);
        }

        protected static CoordinateReferenceSystem forName(String name) {
            return CRS.valueOf(name.toUpperCase(Locale.ROOT)).crs;
        }
    }

    public static enum Orientation {
        LEFT,
        RIGHT;

        public static final Orientation CLOCKWISE = Orientation.LEFT;
        public static final Orientation COUNTER_CLOCKWISE = Orientation.RIGHT;
        public static final Orientation CW = Orientation.LEFT;
        public static final Orientation CCW = Orientation.RIGHT;
    }

    public static final String FIELD_TYPE = "type";
    public static final String FIELD_COORDINATES = "coordinates";
    public static final String FIELD_GEOMETRIES = "geometries";
    public static final String FIELD_ORIENTATION = "orientation";
    public static final String FIELD_CRS = "crs";
    public static final String FIELD_PROPERTIES = "properties";

    protected static final boolean debugEnabled() {
        return LOGGER.isDebugEnabled() || DEBUG;
    }

    /**
     * Enumeration that lists all {@link GeoShapeType}s that can be handled
     */
    public static enum GeoShapeType {
        POINT("point"),
        MULTIPOINT("multipoint"),
        LINESTRING("linestring"),
        MULTILINESTRING("multilinestring"),
        POLYGON("polygon"),
        MULTIPOLYGON("multipolygon"),
        GEOMETRYCOLLECTION("geometrycollection"),
        ENVELOPE("envelope"),
        CIRCLE("circle");

        protected final String shapename;

        private GeoShapeType(String shapename) {
            this.shapename = shapename;
        }

        public static GeoShapeType forName(String geoshapename) {
            String typename = geoshapename.toLowerCase(Locale.ROOT);
            for (GeoShapeType type : values()) {
                if(type.shapename.equals(typename)) {
                    return type;
                }
            }
            throw new ElasticsearchIllegalArgumentException("unknown geo_shape ["+geoshapename+"]");
        }

        public static ShapeBuilder parse(XContentParser parser) throws IOException {
            return parse(parser, null);
        }

        /**
         * Parse the geometry specified by the source document and return a ShapeBuilder instance used to
         * build the actual geometry
         * @param parser - parse utility object including source document
         * @param shapeMapper - field mapper needed for index specific parameters
         * @return ShapeBuilder - a builder instance used to create the geometry
         * @throws IOException
         */
        public static ShapeBuilder parse(XContentParser parser, GeoShapeFieldMapper shapeMapper) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            } else if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("Shape must be an object consisting of type and coordinates");
            }

            GeoShapeType shapeType = null;
            Distance radius = null;
            CoordinateNode node = null;
            GeometryCollectionBuilder geometryCollections = null;
            Orientation requestedOrientation = (shapeMapper == null) ? Orientation.RIGHT : shapeMapper.orientation();
            CoordinateReferenceSystem crs = (shapeMapper == null) ? WGS84 : shapeMapper.crs();

            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();

                    if (FIELD_TYPE.equals(fieldName)) {
                        parser.nextToken();
                        shapeType = GeoShapeType.forName(parser.text());
                    } else if (FIELD_COORDINATES.equals(fieldName)) {
                        parser.nextToken();
                        node = parseCoordinates(parser, crs);
                    } else if (FIELD_GEOMETRIES.equals(fieldName)) {
                        parser.nextToken();
                        geometryCollections = parseGeometries(parser, requestedOrientation, crs);
                    } else if (CircleBuilder.FIELD_RADIUS.equals(fieldName)) {
                        parser.nextToken();
                        radius = Distance.parseDistance(parser.text());
                    } else if (FIELD_ORIENTATION.equals(fieldName)) {
                        parser.nextToken();
                        requestedOrientation = orientationFromString(parser.text());
                    } else if (FIELD_CRS.equals(fieldName)) {
                        parser.nextToken();
                        crs = CRSBuilder.parseCRS(parser);
                    } else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }

            if (shapeType == null) {
                throw new ElasticsearchParseException("Shape type not included");
            } else if (node == null && GeoShapeType.GEOMETRYCOLLECTION != shapeType) {
                throw new ElasticsearchParseException("Coordinates not included");
            } else if (geometryCollections == null && GeoShapeType.GEOMETRYCOLLECTION == shapeType) {
                throw new ElasticsearchParseException("geometries not included");
            } else if (radius != null && GeoShapeType.CIRCLE != shapeType) {
                throw new ElasticsearchParseException("Field [" + CircleBuilder.FIELD_RADIUS + "] is supported for [" + CircleBuilder.TYPE
                        + "] only");
            }

            switch (shapeType) {
                case POINT: return parsePoint(node, crs);
                case MULTIPOINT: return parseMultiPoint(node, crs);
                case LINESTRING: return parseLineString(node, crs);
                case MULTILINESTRING: return parseMultiLine(node, crs);
                case POLYGON: return parsePolygon(node, requestedOrientation, crs);
                case MULTIPOLYGON: return parseMultiPolygon(node, requestedOrientation, crs);
                case CIRCLE: return parseCircle(node, radius);
                case ENVELOPE: return parseEnvelope(node, requestedOrientation, crs);
                case GEOMETRYCOLLECTION: return geometryCollections;
                default:
                    throw new ElasticsearchParseException("Shape type [" + shapeType + "] not included");
            }
        }
        
        protected static void validatePointNode(CoordinateNode node) {
            if (node.isEmpty()) {
                throw new ElasticsearchParseException("Invalid number of points (0) provided when expecting a single coordinate "
                        + "([lat, lng])");
            } else if (node.coordinate == null) {
                if (node.children.isEmpty() == false) {
                    throw new ElasticsearchParseException("multipoint data provided when single point data expected.");
                }
            }
        }

        protected static PointBuilder parsePoint(CoordinateNode node, CoordinateReferenceSystem crs) {
            validatePointNode(node);
            return newPoint(node.coordinate, crs);
        }

        protected static CircleBuilder parseCircle(CoordinateNode coordinates, Distance radius) {
            return newCircleBuilder().center(coordinates.coordinate).radius(radius);
        }

        protected static EnvelopeBuilder parseEnvelope(CoordinateNode coordinates, Orientation orientation, CoordinateReferenceSystem crs) {
            // validate the coordinate array for envelope type
            if (coordinates.children.size() != 2) {
                throw new ElasticsearchParseException("Invalid number of points (" + coordinates.children.size() + ") provided for " +
                        "geo_shape ('envelope') when expecting an array of 2 coordinates");
            }
            // verify coordinate bounds, correct if necessary
            Coordinate uL = coordinates.children.get(0).coordinate;
            Coordinate lR = coordinates.children.get(1).coordinate;
            if (((lR.x < uL.x) || (uL.y < lR.y))) {
                Coordinate uLtmp = uL;
                uL = new Coordinate(Math.min(uL.x, lR.x), Math.max(uL.y, lR.y));
                lR = new Coordinate(Math.max(uLtmp.x, lR.x), Math.min(uLtmp.y, lR.y));
            }
            return newEnvelope(orientation, crs).topLeft(uL).bottomRight(lR);
        }

        protected static void validateMultiPointNode(CoordinateNode coordinates) {
            if (coordinates.children == null || coordinates.children.isEmpty()) {
                if (coordinates.coordinate != null) {
                    throw new ElasticsearchParseException("single coordinate found when expecting an array of " +
                            "coordinates. change type to point or change data to an array of >0 coordinates");
                }
                throw new ElasticsearchParseException("No data provided for multipoint object when expecting " +
                        ">0 points (e.g., [[lat, lng]] or [[lat, lng], ...])");
            } else {
                for (CoordinateNode point : coordinates.children) {
                    validatePointNode(point);
                }
            }
        }

        protected static MultiPointBuilder parseMultiPoint(CoordinateNode coordinates, CoordinateReferenceSystem crs) {
            validateMultiPointNode(coordinates);

            MultiPointBuilder points = new MultiPointBuilder(crs);
            for (CoordinateNode node : coordinates.children) {
                points.point(node.coordinate);
            }
            return points;
        }

        protected static LineStringBuilder parseLineString(CoordinateNode coordinates, CoordinateReferenceSystem crs) {
            /**
             * Per GeoJSON spec (http://geojson.org/geojson-spec.html#linestring)
             * "coordinates" member must be an array of two or more positions
             * LineStringBuilder should throw a graceful exception if < 2 coordinates/points are provided
             */
            if (coordinates.children.size() < 2) {
                throw new ElasticsearchParseException("Invalid number of points in LineString (found " +
                        coordinates.children.size() + " - must be >= 2)");
            }

            LineStringBuilder line = newLineString(crs);
            for (CoordinateNode node : coordinates.children) {
                line.point(node.coordinate);
            }
            return line;
        }

        protected static MultiLineStringBuilder parseMultiLine(CoordinateNode coordinates, CoordinateReferenceSystem crs) {
            MultiLineStringBuilder multiline = newMultiLinestring();
            for (CoordinateNode node : coordinates.children) {
                multiline.linestring(parseLineString(node, crs));
            }
            return multiline;
        }

        protected static LineStringBuilder parseLinearRing(CoordinateNode coordinates, CoordinateReferenceSystem crs) {
            /**
             * Per GeoJSON spec (http://geojson.org/geojson-spec.html#linestring)
             * A LinearRing is closed LineString with 4 or more positions. The first and last positions
             * are equivalent (they represent equivalent points). Though a LinearRing is not explicitly
             * represented as a GeoJSON geometry type, it is referred to in the Polygon geometry type definition.
             */
            if (coordinates.children.size() < 4) {
                throw new ElasticsearchParseException("Invalid number of points in LinearRing (found " +
                        coordinates.children.size() + " - must be >= 4)");
            } else if (!coordinates.children.get(0).coordinate.equals(
                        coordinates.children.get(coordinates.children.size() - 1).coordinate)) {
                throw new ElasticsearchParseException("Invalid LinearRing found (coordinates are not closed)");
            }
            return parseLineString(coordinates, crs);
        }

        protected static PolygonBuilder parsePolygon(CoordinateNode coordinates, Orientation orientation, CoordinateReferenceSystem crs) {
            if (coordinates.children == null || coordinates.children.isEmpty() ||
                    coordinates.children.get(0).children == null || coordinates.children.get(0).children.isEmpty() ) {
                throw new ElasticsearchParseException("Invalid polygon provided. Polygon type must be an array of >=1 LinearRings");
            }

            LineStringBuilder shell = parseLinearRing(coordinates.children.get(0), crs);
            PolygonBuilder polygon = new PolygonBuilder(shell.points, orientation, crs);
            for (int i = 1; i < coordinates.children.size(); i++) {
                polygon.hole(parseLinearRing(coordinates.children.get(i), crs));
            }
            return polygon;
        }

        protected static MultiPolygonBuilder parseMultiPolygon(CoordinateNode coordinates, Orientation orientation, CoordinateReferenceSystem crs) {
            MultiPolygonBuilder polygons = newMultiPolygon(orientation, crs);
            for (CoordinateNode node : coordinates.children) {
                polygons.polygon(parsePolygon(node, orientation, crs));
            }
            return polygons;
        }
        
        /**
         * Parse the geometries array of a GeometryCollection
         *
         * @param parser Parser that will be read from
         * @return Geometry[] geometries of the GeometryCollection
         * @throws IOException Thrown if an error occurs while reading from the XContentParser
         */
        protected static GeometryCollectionBuilder parseGeometries(XContentParser parser, Orientation orientation,
                                                                   CoordinateReferenceSystem crs) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("Geometries must be an array of geojson objects");
            }
        
            XContentParser.Token token = parser.nextToken();
            GeometryCollectionBuilder geometryCollection = newGeometryCollection(orientation, crs);
            while (token != XContentParser.Token.END_ARRAY) {
                ShapeBuilder shapeBuilder = GeoShapeType.parse(parser);
                geometryCollection.shape(shapeBuilder);
                token = parser.nextToken();
            }
        
            return geometryCollection;
        }
    }
}
