package org.mahmoud.lynxes.core;

/**
 * Represents an index entry containing offset, position, length, and checksum.
 * This is used by both Index and SparseIndex classes.
 */
public class IndexEntry {
    private final long offset;
    private final long position;
    private final int length;
    private final int checksum;

    public IndexEntry(long offset, long position, int length, int checksum) {
        this.offset = offset;
        this.position = position;
        this.length = length;
        this.checksum = checksum;
    }

    public long getOffset() {
        return offset;
    }

    public long getPosition() {
        return position;
    }

    public int getLength() {
        return length;
    }

    public int getChecksum() {
        return checksum;
    }

    @Override
    public String toString() {
        return String.format("IndexEntry{offset=%d, position=%d, length=%d, checksum=%d}", 
                           offset, position, length, checksum);
    }
}
