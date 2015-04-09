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

package org.apache.lucene.spatial.prefix;

import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

import java.util.Iterator;

/**
 * A reset'able {@link org.apache.lucene.util.BytesRefIterator} wrapper around
 * an {@link java.util.Iterator} of {@link org.apache.lucene.spatial.prefix.tree.Cell}s.
 *
 * @see org.apache.lucene.spatial.prefix.PrefixTreeStrategy#newCellToBytesRefIterator()
 *
 * @lucene.internal
 */
public class CellToBytesRefIterator implements BytesRefIterator {

  protected Iterator<Cell> cellIter;
  protected BytesRef bytesRef = new BytesRef();

  public void reset(Iterator<Cell> cellIter) {
    this.cellIter = cellIter;
  }

  @Override
  public BytesRef next() {
    if (!cellIter.hasNext()) {
      return null;
    }
    return cellIter.next().getTokenBytesWithLeaf(bytesRef);
  }
}
