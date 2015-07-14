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

import java.io.IOException;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;

/** Package private implementation for the public facing GeoPointDistanceQuery delegate class.
 *
 *    @lucene.experimental
 */
final class GeoPointDistanceQueryImpl extends GeoPointInBBoxQueryImpl {
  private final GeoPointDistanceQuery query;

  GeoPointDistanceQueryImpl(final String field, final GeoPointDistanceQuery q, final double centerLon, final double centerLat, final double radius) {
    this(field, q, GeoPointDistanceQuery.computeBBox(centerLon, centerLat, radius));
  }

  GeoPointDistanceQueryImpl(final String field, final GeoPointDistanceQuery q, final GeoBoundingBox bbox) {
    super(field, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
    query = q;
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
      return GeoUtils.rectCrossesCircle(minLon, minLat, maxLon, maxLat, query.centerLon, query.centerLat, query.radius);
    }

    @Override
    protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectWithinCircle(minLon, minLat, maxLon, maxLat, query.centerLon, query.centerLat, query.radius);
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
    protected final AcceptStatus accept(BytesRef term) {
      // first filter by bounding box
      AcceptStatus status = super.accept(term);
      assert status != AcceptStatus.YES_AND_SEEK;

      if (status != AcceptStatus.YES) {
        return status;
      }

      final long val = NumericUtils.prefixCodedToLong(term);
      final double lon = GeoUtils.mortonUnhashLon(val);
      final double lat = GeoUtils.mortonUnhashLat(val);
      // post-filter by distance
      if (!(SloppyMath.haversin(query.centerLat, query.centerLon, lat, lon) <= (query.radius/1000.0))) {
        return AcceptStatus.NO;
      }

      return AcceptStatus.YES;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeoPointDistanceQueryImpl)) return false;
    if (!super.equals(o)) return false;

    GeoPointDistanceQueryImpl that = (GeoPointDistanceQueryImpl) o;

    if (!query.equals(that.query)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + query.hashCode();
    return result;
  }
}
