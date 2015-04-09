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

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree.PackedQuadCell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {StreamingPrefixTreeStrategy}.java
 * nknize, 4/3/15 2:30 PM
 * <p/>
 * Description:
 */
public class StreamingPrefixTreeStrategy extends RecursivePrefixTreeStrategy {

  public StreamingPrefixTreeStrategy(SpatialPrefixTree grid, String fieldName) {
    super(grid, fieldName);
  }

  @Override
  protected Iterator<Cell> createCellIteratorToIndex(Shape shape, int detailLevel, Iterator<Cell> reuse) {
    return new PrefixTreeIterator(shape);
  }

  protected class PrefixTreeIterator implements Iterator<Cell> {
    private Shape shape;
    private Cell current;
    private Cell next;

    PrefixTreeIterator(Shape shape) {
      super();
      this.shape = shape;
      this.current = ((PackedQuadCell)(grid.getWorldCell())).nextCell();
      this.next = null;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }
      SpatialRelation rel;
      // loop until we're at the end of the quad tree or we hit a relation
      while (current != null /*&& !current.getShapeRel().intersects()*/) {
        rel = current.getShape().relate(shape);
        if (rel == SpatialRelation.DISJOINT) {
          current = ((PackedQuadCell) current).nextCell(false);
        } else if (rel == SpatialRelation.INTERSECTS || rel == SpatialRelation.CONTAINS) {
          current.setShapeRel(rel);
          next = current;
          if (current.getLevel() == grid.getMaxLevels()) {
            current.setLeaf();
          }
          current = ((PackedQuadCell) current).nextCell(true);
          break;
        } else if (rel == SpatialRelation.WITHIN ) {
          current.setLeaf();
          current.setShapeRel(rel);
          next = current;
          current = ((PackedQuadCell) current).nextCell(false);
          break;
        }
      }
      return next != null;
    }

    @Override
    public Cell next() {
      if (next == null) {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
      }
      Cell temp = next;
      next = null;
      return temp;
    }
  }
}
