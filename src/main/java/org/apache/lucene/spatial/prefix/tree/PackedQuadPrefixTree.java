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

package org.apache.lucene.spatial.prefix.tree;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * {PackedQuadPrefixTree}.java
 * nknize, 3/25/15 1:30 PM
 * <p/>
 * Description:
 */
public class PackedQuadPrefixTree extends QuadPrefixTree {
  public static final byte[] QUAD = new byte[] {0x00, 0x01, 0x02, 0x03};
  public static final int MAX_LEVELS_POSSIBLE = 31;

//  public PackedQuadPrefixTree(SpatialContext ctx, Rectangle bounds, int maxLevels) {
//    super(ctx, bounds, maxLevels);
//  }
  public PackedQuadPrefixTree(SpatialContext ctx, int maxLevels) {
    super(ctx, maxLevels);
  }

  @Override
  public Cell getWorldCell() {
    return new PackedQuadCell(0x2L/*, 0*/);
  }
  @Override
  public Cell getCell(Point p, int level) {
    List<Cell> cells = new ArrayList<>(1);
    build(xmid, ymid, 0, cells, 0x2L, ctx.makePoint(p.getX(),p.getY()), level);
    return cells.get(0);//note cells could be longer if p on edge
  }

  protected void build(double x, double y, int level, List<Cell> matches, long term, Shape shape, int maxLevel) {
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    // Z-Order
    // http://en.wikipedia.org/wiki/Z-order_%28curve%29
    checkBattenberg(QUAD[0], x - w, y + h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[1], x + w, y + h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[2], x - w, y - h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[3], x + w, y - h, level, matches, term, shape, maxLevel);

    // possibly consider hilbert curve
    // http://en.wikipedia.org/wiki/Hilbert_curve
    // http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves
    // if we actually use the range property in the query, this could be useful
  }

  protected void checkBattenberg(byte quad, double cx, double cy, int level, List<Cell> matches,
                               long term, Shape shape, int maxLevel) {
    // short-circuit if we find a match for the point (no need to continue recursion)
    if (shape instanceof Point && !matches.isEmpty())
      return;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    SpatialRelation v = shape.relate(ctx.makeRectangle(cx - w, cx + w, cy - h, cy + h));

    if (SpatialRelation.DISJOINT == v) {
      return;
    }

    // set bits for next level
    term <<= 2;  // shift level bit
    term |= (quad<<1); //((++level<<1)-1));

    if (SpatialRelation.CONTAINS == v || (++level >= maxLevel)) {
      matches.add(new PackedQuadCell(term, v.transpose()));
    } else {// SpatialRelation.WITHIN, SpatialRelation.INTERSECTS
      build(cx, cy, level, matches, term, shape, maxLevel);
    }
  }

  @Override
  public Cell readCell(BytesRef term, Cell scratch) {
    PackedQuadCell cell = (PackedQuadCell) scratch;
    if (cell == null)
      cell = (PackedQuadCell) getWorldCell();
    cell.readCell(term);
    return cell;
  }

  public class PackedQuadCell extends QuadCell {
    private long term;

    PackedQuadCell(long term) {
      super(null, 0, 0);
      this.term = term;
      this.b_off = 0;
      this.bytes = longToByteArray(this.term);
      this.b_len = Long.BYTES;
      readLeafAdjust();
    }

    PackedQuadCell(long term, SpatialRelation shapeRel) {
      this(term);
      this.shapeRel = shapeRel;
    }

    @Override
    protected void readCell(BytesRef bytes) {
      shapeRel = null;
      shape = null;
      this.bytes = bytes.bytes;
      this.b_off = bytes.offset;
      this.b_len = (short) bytes.length;
      this.term = longFromByteArray(this.bytes);
      readLeafAdjust();
    }

    /**
     * gets the next cell in lexicographical order
     */
    public Cell nextCell() {
      final int level = getLevel();
      if(level == maxLevels) {
        return null;
      }
      // shift is used to set the level bit (e.g., level 4 = 1000000000)
      // the LSB is the leaf bit
      int shift = (level<<1)+2;
      // to determine whether we descend we need to see if the cell bits
      // are all ones (e.g., at level 2 - 111110)
      boolean descend = (((0x1L<<(shift))-2)^term) == 0;
      // since level is 0 based we need to explicitly set the shift to 3
      long newTerm = (descend) ? 0x1L<< ((level==0) ? 3 : shift+1) : term+0x2L;
      return new PackedQuadCell(newTerm);
    }

    /*public Cell nextCell(boolean descend) {
      final int level = getLevel();
      final boolean maxLevel = level == maxLevels;

      // if descend we move to the first child on the next level
      // to determine if we are at the last sibling we need to check if bit 1 and 2 are set
      if (maxLevel && descend) {
        newTerm = term >> 2;
      }

      long newTerm = term;
      if (descend) {
        newTerm = (level == maxLevels-1) ?
      }

      boolean lastCell = maxLevels

      long newTerm = (descend) ? ((maxLevel & !lastCell) ? ((term>>2)+1) : (term<<2))

      long newTerm = ((descend) ? ((level == maxLevels-1) ? (term<<2) : ((term<<2)|0x1L)) :
          ((term&0x6L) == 0x6L) ? null :
              (term+0x2L));
      return new PackedQuadCell(newTerm);
    }*/

    public boolean isEnd(int level) {
      return (term != 0x2L && (((0x1L<<((level<<1)+2))-2) == term));
    }

    /** WORKS!!! */
//    public Cell nextCell(boolean descend) {
//      final int level = getLevel();
//      // base case: can't go further
//      if (isEnd(level) || isEnd(maxLevels)) {
//        return null;
//      }
//
//      long newTerm = term;
//      // if descend requested && we're not at the maxLevel
//      if (level == 0 || (descend && (level != maxLevels))) {
//        // simple case: shift the term by 2 (next level)
//        newTerm <<= 2;
//      } else {  // we're not descending or we can't descend
//        // simple case: we're not at the last sibling so move to the next sibling
//        if ((newTerm&0x6L) != 0x6L) {
//          newTerm += 0x2L;
//        } else { // we're at the last sibling...descend
//            // have to descend
//          newTerm = (newTerm+0x2L);
//          final int tz = Long.numberOfTrailingZeros(newTerm);
//          newTerm >>>= (tz-(0x1<<(((tz&0x1)==0)?1:0)));
//        }
//      }
//      return new PackedQuadCell(newTerm);
//    }

    public Cell nextCell(boolean descend) {
      final int level = getLevel();
      // base case: can't go further
      if ( (!descend && isEnd(level)) || isEnd(maxLevels)) {
        return null;
      }

      long newTerm;
      boolean isLeaf = (term&0x1L)==0x1L;
      // if descend requested && we're not at the maxLevel
      if ((descend && !isLeaf && (level != maxLevels)) || level == 0) {
        // simple case: shift the term by 2 (next level)
        newTerm = term<<2;
      } else {  // we're not descending or we can't descend
        newTerm = ((isLeaf) ? (term-1) : term) + 0x2L;
        // we're at the last sibling...force descend
        if ((term&0x6L) == 0x6L) {
          final int tz = Long.numberOfTrailingZeros(newTerm);
          newTerm >>>= (tz-(0x1<<(((tz&0x1)==0)?1:0)));
        }
      }
      return new PackedQuadCell(newTerm);
    }

    @Override
    protected void readLeafAdjust() {
      isLeaf = ((0x1L)&term) == 0x1L;
      if (getLevel() == getMaxLevels())
        isLeaf = true;
    }

    @Override
    public BytesRef getTokenBytesWithLeaf(BytesRef result) {
      if (isLeaf) {
        term |= 0x1L;
      }
      return getTokenBytesNoLeaf(result);
    }

    @Override
    public BytesRef getTokenBytesNoLeaf(BytesRef result) {
      if (result == null)
        return new BytesRef(bytes, b_off, b_len);
      result.bytes = longToByteArray(this.term);
      result.offset = 0;
      result.length = result.bytes.length;
      return result;
    }

    @Override
    public int compareToNoLeaf(Cell fromCell) {
      LegacyCell b = (LegacyCell) fromCell;
      return compare(bytes, b_off, b_len, b.bytes, b.b_off, b.b_len)-1;
//      PackedQuadCell b = (PackedQuadCell) fromCell;
//      int shift = (this.getLevel() - b.getLevel())<<1;
//      long fromTerm = b.term << shift;
//      long indexedTerm = this.term  & (0x0EL<<shift);
//      byte[] visitBytes = longToByteArray(fromTerm);
//      byte[] indexedBytes = longToByteArray(indexedTerm);
//      int comparison = compare(indexedBytes, b_off, b_len, visitBytes, b.b_off, b.b_len);
//      return (comparison==0) ? comparison : comparison-1;
    }

    @Override
    public int getLevel() {
      int l = ((64-Long.numberOfLeadingZeros(term))>>1)-1;
      return l;
    }

    @Override
    protected Collection<Cell> getSubCells() {
      List<Cell> cells = new ArrayList<>(4);
      cells.add(new PackedQuadCell(concat(QUAD[0]), null));
      cells.add(new PackedQuadCell(concat(QUAD[1]), null));
      cells.add(new PackedQuadCell(concat(QUAD[2]), null));
      cells.add(new PackedQuadCell(concat(QUAD[3]), null));
      return cells;
    }

    @Override
    protected QuadCell getSubCell(Point p) {
      return (PackedQuadCell) PackedQuadPrefixTree.this.getCell(p, getLevel() + 1);//not performant!
    }


    protected long concat(byte postfix) {
      // extra leaf bit
      return (this.term<<2)|(postfix<<1);
    }

    /**
     *
     * @return level of this quad cell
     */
    private int calcLevel() {
      long c = this.term;
      int level = 0;
      for (; (c & 0x8000000000000000L) == 0L; ++level, c <<= 1) ;

      return ((64-level)>>1)-1;
    }

    @Override
    protected Rectangle makeShape() {
      double xmin = PackedQuadPrefixTree.this.xmin;
      double ymin = PackedQuadPrefixTree.this.ymin;
      int level = calcLevel();

      byte b;
      for (short i = (short) ((level << 1) - 1), l = 0; i > 0; i -= 2, ++l) {
        b = (byte) ((this.term >>> i) & 0x3L);

        switch (b) {
          case 0x00:
            ymin += levelH[l];
            break;
          case 0x01:
            xmin += levelW[l];
            ymin += levelH[l];
            break;
          case 0x02:
            break;//nothing really
          case 0x03:
            xmin += levelW[l];
            break;
          default:
            throw new RuntimeException("unexpected quadrant");
        }
      }

      double width, height;
      if (level > 0) {
        width = levelW[level - 1];
        height = levelH[level - 1];
      } else {
        width = gridW;
        height = gridH;
      }
      return new RectangleImpl(xmin, xmin + width, ymin, ymin + height, ctx);
    }

    private byte[] longToByteArray(long value) {
      byte[] result = new byte[8];
      for(int i = 7; i >= 0; --i) {
        result[i] = (byte)((int)(value & 255L));
        value >>= 8;
      }
      return result;
    }

    private long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
      return ((long)b1 & 255L) << 56 | ((long)b2 & 255L) << 48 | ((long)b3 & 255L) << 40
          | ((long)b4 & 255L) << 32 | ((long)b5 & 255L) << 24 | ((long)b6 & 255L) << 16
          | ((long)b7 & 255L) << 8 | (long)b8 & 255L;
    }

    private long longFromByteArray(byte[] bytes) {
      assert bytes.length >= 8;
      return fromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);
    }

    @Override
    public String toString() {
      //return longAsString(this.code);
      String s = "";
      for(int i = 0; i < Long.numberOfLeadingZeros(term); i++) {
        s+='0';
      }
      if (term != 0)
        s += Long.toBinaryString(term);
      return s;
    }
  } // PackedQuadCell

  public static void main(String[] args) {
    PackedQuadPrefixTree pt = new PackedQuadPrefixTree(SpatialContext.GEO, 26);
    PackedQuadCell qc = (PackedQuadCell)pt.getWorldCell();
    Random r = new Random();
    do {
      boolean descend = r.nextBoolean();
      qc = (PackedQuadCell)qc.nextCell(descend);
      //System.out.println(qc);
    } while (qc != null);
  }
}
