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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;


/**
 * Tests for {@link GeoHashUtils}
 */
public class GeoHashTests extends ElasticsearchTestCase {


    @Test
    public void testGeohashAsLongRoutines()  {
        
        //Ensure that for all points at all supported levels of precision
        // that the long encoding of a geohash is compatible with its 
        // String based counterpart
        for (double lat=-90;lat<90;lat++)
        {
            for (double lng=-180;lng<180;lng++)
            {
                for(int p=1;p<=12;p++)
                {
                    long geoAsLong = GeoHashUtils.encodeAsLong(lat,lng,p);
                    String geohash = GeoHashUtils.encode(lat,lng,p);
                    
                    String geohashFromLong=GeoHashUtils.toString(geoAsLong);
                    assertEquals(geohash, geohashFromLong);
                    GeoPoint pos=GeoHashUtils.decode(geohash);
                    GeoPoint pos2=GeoHashUtils.decode(geoAsLong);
                    assertEquals(pos, pos2);
                }
            }
            
        }        
    }


    @Test
    public void testGeohashBoundary() {
        double lon = 180.0;
        // double lon = -96.13895416259766;
        double lat = 32.917349963900215;

        Long geoAsLong = GeoHashUtils.encodeAsLong(lat,lon,12);
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(geoAsLong);
        BitSet bs = fromByteArray(bb.array());
        String geohashFromLong=GeoHashUtils.toString(geoAsLong);

        GeoPoint p = GeoHashUtils.decode(geoAsLong);

        geohashFromLong.concat("");

    }


    // Returns a bitset containing the values in bytes.
    public static BitSet fromByteArray(byte[] bytes) {
        BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
                bits.set(i);
            }
        }
        return bits;
    }
}
