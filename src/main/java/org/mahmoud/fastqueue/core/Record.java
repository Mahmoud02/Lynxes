package org.mahmoud.fastqueue.core;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a message record in the log.
 * Each record contains an offset (unique ID), timestamp, and payload data.
 */
public class Record {
    private final long offset;
    private final long timestamp;
    private final byte[] data;
    private final int checksum;

    /**
     * Creates a new record with the given offset, timestamp, and data.
     * 
     * @param offset Unique identifier for this record
     * @param timestamp When this record was created (milliseconds since epoch)
     * @param data The actual message payload
     */
    public Record(long offset, long timestamp, byte[] data) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.data = data != null ? data.clone() : new byte[0];
        this.checksum = calculateChecksum();
    }

    /**
     * Creates a new record with current timestamp.
     * 
     * @param offset Unique identifier for this record
     * @param data The actual message payload
     */
    public Record(long offset, byte[] data) {
        this(offset, System.currentTimeMillis(), data);
    }

    /**
     * Creates a new record with current timestamp and offset.
     * 
     * @param data The actual message payload
     */
    public Record(byte[] data) {
        this(0, System.currentTimeMillis(), data);
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getData() {
        return data.clone(); // Return a copy to prevent external modification
    }

    public int getDataLength() {
        return data.length;
    }

    public int getChecksum() {
        return checksum;
    }

    /**
     * Calculates a simple checksum for data integrity verification.
     * Uses a simple XOR-based checksum for performance.
     */
    private int calculateChecksum() {
        int checksum = 0;
        for (byte b : data) {
            checksum ^= b;
        }
        return checksum;
    }

    /**
     * Verifies the integrity of this record by recalculating and comparing checksums.
     * 
     * @return true if the record is valid, false otherwise
     */
    public boolean isValid() {
        return calculateChecksum() == checksum;
    }

    /**
     * Serializes this record to a byte array for storage.
     * Format: [Length:4][Timestamp:8][Checksum:4][Data:Length]
     * 
     * @return Serialized record as byte array
     */
    public byte[] serialize() {
        int totalLength = 4 + 8 + 4 + data.length; // Length + Timestamp + Checksum + Data
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        
        buffer.putInt(data.length);           // Data length
        buffer.putLong(timestamp);            // Timestamp
        buffer.putInt(checksum);              // Checksum
        buffer.put(data);                     // Actual data
        
        return buffer.array();
    }

    /**
     * Deserializes a record from a byte array.
     * 
     * @param serializedData The serialized record data
     * @param offset The offset for this record (not stored in serialized data)
     * @return Deserialized Record object
     * @throws IllegalArgumentException if the data is malformed
     */
    public static Record deserialize(byte[] serializedData, long offset) {
        if (serializedData == null || serializedData.length < 16) { // Minimum: 4 + 8 + 4
            throw new IllegalArgumentException("Invalid serialized data");
        }

        ByteBuffer buffer = ByteBuffer.wrap(serializedData);
        
        int dataLength = buffer.getInt();
        long timestamp = buffer.getLong();
        int checksum = buffer.getInt();
        
        if (dataLength < 0 || dataLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid data length: " + dataLength);
        }
        
        byte[] data = new byte[dataLength];
        buffer.get(data);
        
        Record record = new Record(offset, timestamp, data);
        
        // Verify checksum
        if (record.checksum != checksum) {
            throw new IllegalArgumentException("Checksum mismatch");
        }
        
        return record;
    }

    /**
     * Gets the total serialized size of this record.
     * 
     * @return Size in bytes
     */
    public int getSerializedSize() {
        return 4 + 8 + 4 + data.length; // Length + Timestamp + Checksum + Data
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return offset == record.offset &&
               timestamp == record.timestamp &&
               checksum == record.checksum &&
               Objects.equals(data, record.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, data, checksum);
    }

    @Override
    public String toString() {
        return String.format("Record{offset=%d, timestamp=%d, dataLength=%d, checksum=%d}", 
                           offset, timestamp, data.length, checksum);
    }
}
