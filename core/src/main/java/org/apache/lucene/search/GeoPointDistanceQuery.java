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

package org.apache.lucene.search;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;

import java.io.IOException;

/** Implements a simple point distance query on a GeoPoint field. This is based on
 * {@link GeoPointInPolygonQuery} and is implemented using a two phase approach. First,
 * like {@link GeoPointInBBoxQuery} candidate terms are queried using the numeric ranges based on
 * the morton codes of the min and max lat/lon pairs that intersect a polygonal representation of the
 * circle (see {@link org.apache.lucene.util.GeoUtils#circleToPoly}. Terms
 * passing this initial filter are then passed to a secondary filter that verifies whether the
 * decoded lat/lon point fall within the specified query distance. All value comparisons are subject
 * to the same precision tolerance defined in {@value org.apache.lucene.util.GeoUtils#TOLERANCE}
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
public class GeoPointDistanceQuery extends GeoPointInBBoxQuery {
  private final double centerLon;
  private final double centerLat;
  private final double radius;

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

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    return new GeoPointRadiusTermsEnum(terms.iterator(), atts, minLon, minLat, maxLon, maxLat);
  }

  private final class GeoPointRadiusTermsEnum extends GeoPointTermsEnum {
    GeoPointRadiusTermsEnum(final TermsEnum tenum, AttributeSource atts, final double minLon, final double minLat,
                        final double maxLon, final double maxLat) {
      super(tenum, atts, minLon, minLat, maxLon, maxLat);
    }

    @Override
    protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectCrossesCircle(minLon, minLat, maxLon, maxLat, centerLon, centerLat, radius);
    }

    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectWithinCircle(minLon, minLat, maxLon, maxLat, centerLon, centerLat, radius);
    }

    /**
     * The two-phase query approach. The parent
     * {@link org.apache.lucene.search.GeoPointTermsEnum#accept} method is called to match
     * encoded terms that fall within the bounding box of the polygon. Those documents that pass the initial
     * bounding box filter are then compared to the provided polygon using the
     * {@link org.apache.lucene.util.GeoUtils#pointInPolygon} method.
     *
     * @param term term for candidate document
     * @return match status
     */
    @Override
    protected final FilteredTermsEnum.AcceptStatus accept(BytesRef term) {
      // first filter by bounding box
      FilteredTermsEnum.AcceptStatus status = super.accept(term);
      assert status != FilteredTermsEnum.AcceptStatus.YES_AND_SEEK;

      if (status != FilteredTermsEnum.AcceptStatus.YES) {
        return status;
      }

      final long val = NumericUtils.prefixCodedToLong(term);
      final double lon = GeoUtils.mortonUnhashLon(val);
      final double lat = GeoUtils.mortonUnhashLat(val);
      // post-filter by distance
      if (!(SloppyMath.haversin(centerLat, centerLon, lat, lon) <= radius)) {
        return FilteredTermsEnum.AcceptStatus.NO;
      }

      return FilteredTermsEnum.AcceptStatus.YES;
    }
  }

  protected static GeoBoundingBox computeBBox(final double centerLon, final double centerLat, final double radius) {
    final double lonDistDeg = GeoDistanceUtils.distanceToDegreesLon(centerLat, radius);
    final double latDistDeg = GeoDistanceUtils.distanceToDegreesLat(centerLat, radius);

    return new GeoBoundingBox(centerLon - lonDistDeg, centerLon + lonDistDeg, centerLat - latDistDeg, centerLat + latDistDeg);
  }
}
