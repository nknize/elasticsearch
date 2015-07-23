package org.apache.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.ToStringUtils;

/** Implements a simple point distance query on a GeoPoint field. This is based on
 * {@link org.apache.lucene.search.GeoPointInBBoxQuery} and is implemented using a two phase approach. First,
 * like {@link GeoPointInBBoxQueryImpl} candidate terms are queried using the numeric ranges based on
 * the morton codes of the min and max lat/lon pairs that intersect the boundary of the point-radius
 * circle (see {@link org.apache.lucene.util.GeoUtils#lineCrossesPointRadius}. Terms
 * passing this initial filter are then passed to a secondary filter that verifies whether the
 * decoded lat/lon point fall within the specified query distance (see {@link org.apache.lucene.util.SloppyMath#haversin}.
 * All morton value comparisons are subject to the same precision tolerance defined in
 * {@value org.apache.lucene.util.GeoUtils#TOLERANCE} and distance comparisons are subject to the accuracy of the
 * haversine formula (from R.W. Sinnott, "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159)
 *
 * NOTE: This query works best for point-radius queries that do not cross the dateline, there is a penalty for crossing
 * the dateline as the bounding box is effectively split into two separate queries, and the point-radius is converted
 * to a euclidean spherical search to handle a wrapping coordinate system (TODO split the point radius at the dateline?)
 *
 * This query also currently uses haversine which is a sloppy distance calculation. For large queries one can expect
 * upwards of 400m error. Vincenty shrinks this to ~40m error but pays a penalty for computing using the spheroid
 *
 *    @lucene.experimental
 */
public final class GeoPointDistanceQuery extends GeoPointInBBoxQuery {
    protected final double centerLon;
    protected final double centerLat;
    protected final double radius;

    public GeoPointDistanceQuery(final String field, final double centerLon, final double centerLat, final double radius) {
        this(field, computeBBox(centerLon, centerLat, radius), centerLon, centerLat, radius);
    }

    private GeoPointDistanceQuery(final String field, GeoBoundingBox bbox, final double centerLon,
                                  final double centerLat, final double radius) {
        super(field, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
        this.centerLon = centerLon;
        this.centerLat = centerLat;
        this.radius = radius;
    }

    @Override
    public Query rewrite(IndexReader reader) {
        if (maxLon < minLon) {
            BooleanQuery q = new BooleanQuery(true);

            GeoPointDistanceQueryImpl left = new GeoPointDistanceQueryImpl(field, this, new GeoBoundingBox(-180.0D, maxLon,
                    minLat, maxLat));
            left.setBoost(getBoost());
            q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));
            GeoPointDistanceQueryImpl right = new GeoPointDistanceQueryImpl(field, this, new GeoBoundingBox(minLon, 180.0D,
                    minLat, maxLat));
            right.setBoost(getBoost());
            q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
            return q;
        }
        return new GeoPointDistanceQueryImpl(field, this, centerLon, centerLat, radius);
    }

    protected static GeoBoundingBox computeBBox(final double centerLon, final double centerLat, final double radius) {
        final double lonDistDeg = GeoDistanceUtils.distanceToDegreesLon(centerLat, radius);
        final double latDistDeg = GeoDistanceUtils.distanceToDegreesLat(centerLat, radius);

        return new GeoBoundingBox(GeoUtils.normalizeLon(centerLon - lonDistDeg), GeoUtils.normalizeLon(centerLon + lonDistDeg),
                GeoUtils.normalizeLat(centerLat - latDistDeg), GeoUtils.normalizeLat(centerLat + latDistDeg));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GeoPointDistanceQuery)) return false;
        if (!super.equals(o)) return false;

        GeoPointDistanceQuery that = (GeoPointDistanceQuery) o;

        if (Double.compare(that.centerLat, centerLat) != 0) return false;
        if (Double.compare(that.centerLon, centerLon) != 0) return false;
        if (Double.compare(that.radius, radius) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(centerLon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(centerLat);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radius);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(':');
        if (!this.field.equals(field)) {
            sb.append(" field=");
            sb.append(this.field);
            sb.append(':');
        }
        return sb.append( " Center: [")
                .append(centerLon)
                .append(',')
                .append(centerLat)
                .append(']')
                .append(" Distance: ")
                .append(radius)
                .append(" km")
                .append(" Lower Left: [")
                .append(minLon)
                .append(',')
                .append(minLat)
                .append(']')
                .append(" Upper Right: [")
                .append(maxLon)
                .append(',')
                .append(maxLat)
                .append("]")
                .append(ToStringUtils.boost(getBoost()))
                .toString();
    }
}
