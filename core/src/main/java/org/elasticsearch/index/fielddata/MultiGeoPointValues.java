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
package org.elasticsearch.index.fielddata;

import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.geo.GeoPoint;

/**
 * A stateful lightweight per document set of {@link GeoPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   GeoPointValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoPoint value = values.valueAt(i);
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public abstract class MultiGeoPointValues {

    /**
     * Creates a new {@link MultiGeoPointValues} instance
     */
    protected MultiGeoPointValues() {
    }

    /**
     * Sets iteration to the specified docID.
     * @param docId document ID
     *
     * @see #valueAt(int)
     * @see #count()
     */
    public abstract void setDocument(int docId);

    /**
     * Return the number of geo points the current document has.
     */
    public abstract int count();

    /**
     * Return the <code>i-th</code> value associated with the current document.
     * Behavior is undefined when <code>i</code> is undefined or greater than
     * or equal to {@link #count()}.
     *
     * Note: the returned {@link GeoPoint} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #setDocument(int)}.
     */
    public abstract GeoPoint valueAt(int i);

    /**
     * Return a {@link SortedNumericDoubleValues} instance that returns the distances to a list of geo-points for each document.
     */
    public static SortedNumericDoubleValues distanceValues(final MultiGeoPointValues geoPointValues, final GeoPoint... sourceLoc) {
        final GeoPointValues singleValues = FieldData.unwrapSingleton(geoPointValues);
        if (singleValues != null && sourceLoc.length == 1) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(geoPointValues);
            return FieldData.singleton(new NumericDoubleValues() {

                @Override
                public double get(int docID) {
                    if (docsWithField != null && !docsWithField.get(docID)) {
                        return 0d;
                    }
                    final GeoPoint point = singleValues.get(docID);
                    return GeoDistanceUtils.haversin(point.lat(), point.lon(), sourceLoc[0].lat(), sourceLoc[0].lon());
                }

            }, docsWithField);
        } else {
            return new SortingNumericDoubleValues() {

                @Override
                public void setDocument(int doc) {
                    geoPointValues.setDocument(doc);
                    resize(geoPointValues.count());
                    int valueCounter = 0;
                    for (GeoPoint source : sourceLoc) {
                        for (int i = 0; i < geoPointValues.count(); ++i) {
                            final GeoPoint point = geoPointValues.valueAt(i);
                            values[valueCounter] = GeoDistanceUtils.haversin(point.lat(), point.lon(), source.lat(), source.lon());
                            valueCounter++;
                        }
                    }
                    sort();
                }
            };
        }
    }

}
