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

package org.apache.lucene.util;

/**
 * {GeoHashUtils}.java
 * nknize, 7/23/15 1:00 PM
 * <p>
 * Description:
 */
public class GeoHashUtils {
    public static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    public static final String BASE_32_STRING = new String(BASE_32);

    public static final int PRECISION = 12;

    /**
     * Encode using the geohash based long format (lat/lon interleaved, least significant 4 bits = level)
     */
    public static final long longEncode(final double lon, final double lat, final int level) {
        // shift to appropriate level
        final short msf = (short)(((12 - level) * 5) + 2);
        return ((GeoUtils.flipFlop(GeoUtils.mortonHash(lon, lat)) >>> msf) << 4) | level;
    }

    public static final long longEncode(final String hash) {
        int level = hash.length()-1;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            l |= (b<<(level--*5));
        }
        return (l<<4)|hash.length();
    }

    public static final String stringEncode(long geoHashLong) {
        int level = (int)geoHashLong&15;
        geoHashLong >>>= 4;
        char[] chars = new char[level];
        do {
            chars[--level] = BASE_32[(int)(geoHashLong&31L)];
            geoHashLong>>>=5;
        } while(level > 0);

        return new String(chars);
    }

    public static final String stringEncode(final double lon, final double lat) {
        return stringEncode(lon, lat, 12);
    }

    public static final String stringEncode(final double lon, final double lat, final int level) {
        // bit twiddle to geohash (since geohash is a swapped (lon/lat) encoding)
        final long hashedVal = GeoUtils.flipFlop(GeoUtils.mortonHash(lon, lat));

        StringBuilder geoHash = new StringBuilder();
        short precision = 0;
        final short msf = (GeoUtils.BITS<<1)-5;
        long mask = 31L<<msf;
        do {
            geoHash.append(BASE_32[(int)((mask & hashedVal)>>>(msf-(precision*5)))]);
            // next 5 bits
            mask >>>= 5;
        } while (++precision < level);
        return geoHash.toString();
    }

    public static final String stringEncodeFromMortonLong(final long hashedVal) throws Exception {
        return stringEncode(hashedVal, 12);
    }

    public static final String stringEncodeFromMortonLong(long hashedVal, final int level) {
        // bit twiddle to geohash (since geohash is a swapped (lon/lat) encoding)
        hashedVal = GeoUtils.flipFlop(hashedVal);

        StringBuilder geoHash = new StringBuilder();
        short precision = 0;
        final short msf = (GeoUtils.BITS<<1)-5;
        long mask = 31L<<msf;
        do {
            geoHash.append(BASE_32[(int)((mask & hashedVal)>>>(msf-(precision*5)))]);
            // next 5 bits
            mask >>>= 5;
        } while (++precision < level);
        return geoHash.toString();
    }

//    public static final long mortonEncode(final String hash) {
//        int level = hash.length()-1;
//        long b;
//        long l = 0L;
//        for(char c : hash.toCharArray()) {
//            b = (long)(BASE_32_STRING.indexOf(c));
//            l |= (b<<((level--*5)+2));
//        }
//        return GeoUtils.flipFlop(l);
//    }

    public static final long mortonEncode(final String hash) {
        int level = 11;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            l |= (b<<((level--*5)+2));
        }
        return GeoUtils.flipFlop(l);
    }

    public static final long mortonEncode(final long geoHashLong) {
        final int level = (int)(geoHashLong&15);
        return GeoUtils.flipFlop((geoHashLong >>> 4) << (((level - 1) * 5) + 2));
    }
}
