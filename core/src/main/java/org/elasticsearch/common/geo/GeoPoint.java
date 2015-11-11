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

import org.apache.lucene.util.*;
import org.apache.lucene.util.GeoUtils;

/**
 * Immutable by default
 */
abstract public class GeoPoint {
    private double lat;
    private double lon;

    // simple constructor
    private GeoPoint() {
    }

    // constructor with value
    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    // getters
    public final double lat() {
        return this.lat;
    }

    public final double lon() {
        return this.lon;
    }

    public final String geohash() {
        return GeoHashUtils.stringEncode(lon, lat);
    }

    public final String getGeohash() {
        return GeoHashUtils.stringEncode(lon, lat);
    }

    private boolean isMutable() {
        return this instanceof Mutable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoPoint geoPoint = (GeoPoint) o;

        if (Double.compare(geoPoint.lat, lat) != 0) return false;
        if (Double.compare(geoPoint.lon, lon) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = Long.hashCode(temp);
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + Long.hashCode(temp);
        return result;
    }

    @Override
    public String toString() {
        return "[" + lat + ", " + lon + "]";
    }

    /**
     * Immutable GeoPoint
     */
    public static class Immutable extends GeoPoint {
        private Immutable() {
            super();
        }

        private Immutable(double lat, double lon) {
            super(lat, lon);
        }
    }

    /**
     * Mutable GeoPoint
     */
    public static class Mutable extends GeoPoint {
        // simple xtor
        private Mutable() {
            super();
        }

        // xtor with value
        private Mutable(double lat, double lon) {
            super(lat, lon);
        }

        // xtor with template
        public Mutable(Mutable template) {
            this(template.lat(), template.lon());
        }

        public Mutable(String value) {
            this.resetFromString(value);
        }

        // setters
        public final void lat(double lat) {
            super.lat = lat;
        }

        // set lon
        public final void lon(double lon) {
            super.lon = lon;
        }

        public Mutable reset(double lat, double lon) {
            super.lat = lat;
            super.lon = lon;
            return this;
        }

        public Mutable resetLat(double lat) {
            super.lat = lat;
            return this;
        }

        public Mutable resetLon(double lon) {
            super.lon = lon;
            return this;
        }

        public Mutable resetFromString(String value) {
            int comma = value.indexOf(',');
            if (comma != -1) {
                super.lat = Double.parseDouble(value.substring(0, comma).trim());
                super.lon = Double.parseDouble(value.substring(comma + 1).trim());
            } else {
                resetFromGeoHash(value);
            }
            return this;
        }

        public Mutable resetFromIndexHash(long hash) {
            super.lon = org.apache.lucene.util.GeoUtils.mortonUnhashLon(hash);
            super.lat = GeoUtils.mortonUnhashLat(hash);
            return this;
        }

        public Mutable resetFromGeoHash(String geohash) {
            final long hash = GeoHashUtils.mortonEncode(geohash);
            return this.reset(org.apache.lucene.util.GeoUtils.mortonUnhashLat(hash), org.apache.lucene.util.GeoUtils.mortonUnhashLon(hash));
        }

        public Mutable resetFromGeoHash(long geohashLong) {
            final int level = (int)(12 - (geohashLong&15));
            return this.resetFromIndexHash(BitUtil.flipFlop((geohashLong >>> 4) << ((level * 5) + 2)));
        }
    }

    // factory creator for immutable
    public static final Immutable immutable(double lat, double lon) {
        return new Immutable(lat, lon);
    }

    // factory creator for mutable
    public static final Mutable mutable() {
        return new Mutable();
    }

    public static final Mutable mutable(double lat, double lon) {
        return new Mutable(lat, lon);
    }

    public static final Mutable mutable(String value) {
        return new Mutable(value);
    }

    public static final Mutable mutable(GeoPoint point) {
        return new Mutable(point.lat, point.lon);
    }
}
