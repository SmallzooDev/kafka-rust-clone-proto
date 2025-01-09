## Record Batch #1

```
00 00 00 00 00 00 00 00 # Base Offset
00 00 00 4f # Batch Length
00 00 00 01 # Partition Leader Epoch
02 # Magic Byte
b0 69 45 7c # CRC
00 00 # Attributes
00 00 00 00 Last Offset Delta
00 00 01 91 e0 5a f8 18 # Base Timestamp
00 00 01 91 e0 5a f8 18 # Max Timestamp
ff ff ff ff ff ff ff ff # Producer ID
ff ff # Producer Epoch
ff ff ff ff # Base Sequence
00 00 00 01 # Records Length
```
### Base Offset
Base Offset is a 8-byte big-endian integer indicating the offset of the first record in this batch.

In this case, the value is `0x00`, which is `0` in decimal, indicating that this is the first batch of records.

 00 00 00 00 00 00 00 00

### Batch Length
Batch Length is a 4-byte big-endian integer indicating the length of the entire record batch in bytes.

This value excludes the Base Offset (8 bytes) and the Batch Length (4 bytes) itself, but includes all other bytes in the record batch.

In this case, the value is `0x4f`, which is `79` in decimal.

00 00 00 4f
### Partition Leader Epoch
Partition Leader Epoch is a 4-byte big-endian integer indicating the epoch of the leader for this partition. It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes. This value is used to detect out of order writes.

In this case, the value is `0x01`, which is `1` in decimal.

00 00 00 01
### Magic Byte
Magic Byte is a 1-byte big-endian integer indicating the version of the record batch format. This value is used to evolve the record batch format in a backward-compatible way.

In this case, the value is `0x02`, which is `2` in decimal.

02
  

### CRC
CRC is a 4-byte big-endian integer indicating the CRC32-C checksum of the record batch.

The CRC is computed over the data following the CRC field to the end of the record batch. The CRC32-C (Castagnoli) polynomial is used for the computation.

In this case, the value is `0xb069457c`, which is `-1335278212` in decimal.

b0 69 45 7c
  

### Attributes
Attributes is a 2-byte big-endian integer indicating the attributes of the record batch.

Attributes is a bitmask of the following flags:

- bit 0~2:
    - 0: no compression
    - 1: gzip
    - 2: snappy
    - 3: lz4
    - 4: zstd
- bit 3: timestampType
- bit 4: isTransactional (0 means not transactional)
- bit 5: isControlBatch (0 means not a control batch)
- bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
- bit 7~15: unused

In this case, the value is `0x00`, which is `0` in decimal.
00 00 
  

### Last Offset Delta
Last Offset Delta is a 4-byte big-endian integer indicating the difference between the last offset of this record batch and the base offset.

In this case, the value is `0x00`, which is `0` in decimal, indicating that the last offset of this record batch is `0` higher than the base offset, so there is 1 record in the recordBatch.

00 00 00 00
  

### Base Timestamp
Base Timestamp is a 8-byte big-endian integer indicating the timestamp of the first record in this batch.

In this case, the value is `0x191e05af818`, which is `1726045943832` in decimal. This is an unix timestamp in milliseconds, which is `2024-09-11 09:12:23.832` in UTC.

00 00 01 91 e0 5a f8 18
  

### Max Timestamp
Max Timestamp is a 8-byte big-endian integer indicating the maximum timestamp of the records in this batch.

In this case, the value is `0x191e05af818`, which is `1726045943832` in decimal. This is an unix timestamp in milliseconds, which is `2024-09-11 09:12:23.832` in UTC.

00 00 01 91 e0 5a f8 18

### Producer ID
Producer ID is a 8-byte big-endian integer indicating the ID of the producer that produced the records in this batch.

In this case, the value is `0xffffffffffffffff`, which is `-1` in decimal. This is a special value that indicates that the producer ID is not set.

ff ff ff ff ff ff ff ff
  

### Producer Epoch
Producer Epoch is a 2-byte big-endian integer indicating the epoch of the producer that produced the records in this batch.

In this case, the value is `0xffff`, which is `-1` in decimal. This is a special value that indicates that the producer epoch is not applicable.

ff ff
  

### Base Sequence
Base Sequence is a 4-byte big-endian integer indicating the sequence number of the first record in a batch. It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.

In this case, the value is `0xffffffff`, which is `-1` in decimal.

ff ff ff ff
  

### Records Length
Records Length is a 4-byte big-endian integer indicating the number of records in this batch.

In this case, the value is `0x01`, which is `1` in decimal, indicating that there is 1 record in the recordBatch.

00 00 00 01
  

## Record #1
```
3a # Length
00 # Attributes
00 # Timestamp Delta
00 # Offset Delta
01 # Key Length
2e # Value Length
... # Value (figureout above)
00 # Headers Array Count

```
### Length
calculated from the attributes field to the end of the record.
In this case, the value is `0x3a`, which is `29` in decimal after parsing.

3a
### Attributes
Attributes is a 1-byte big-endian integer indicating the attributes of the record. Currently, this field is unused in the protocol.

In this case, the value is `0x00`, which is `0` in decimal after parsing.

00
### Timestamp Delta
Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.

In this case, the value is `0x00`, which is `0` in decimal after parsing.

00
### Offset Delta
Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.

In this case, the value is `0x00`, which is `0` in decimal after parsing.

00
  

### Key Length
Key Length is a signed variable size integer indicating the length of the key of the record.

In this case, the value is `0x01`, which is `-1` in decimal after parsing. This is a special value that indicates that the key is null.

01
### Key
Key is a byte array indicating the key of the record.

In this case, the key is null.
  

### Value Length
Value Length is a signed variable size integer indicating the length of the value of the record.

In this case, the value is `0x2e`, which is `23` in decimal after parsing.
2e
  

### Value (Feature Level Record)
```
01 # Frame Version 
0c # Type
00 # Version
11 # Name Length
6d 65 74 61 64 61 74 61 2e 76 65 72 73 69 6f 6e # Name
00 14 # Feature Level
00 # Tagged Fields Count
``` 
#### Frame Version
Frame Version is a 1-byte big-endian integer indicating the version of the format of the record.

In this case, the value is `0x01`, which is `1` in decimal.

01
#### Type
Type is a 1-byte big-endian integer indicating the type of the record.

In this case, the value is `0x0c`, which is `12` in decimal, indicating that this is a Feature Level Record.

0c 
#### Version
Version is a 1-byte big-endian integer indicating the version of the feature level record.

In this case, the value is `0x01`, which is `1` in decimal.

00

#### Name length
Name length is a unsigned variable size integer indicating the length of the name. But, as name is a compact string, the length of the name is always length - 1.

In this case, the value is `0x11`, which is `17` in decimal, indicating that the length of the name is `16`.

11
#### Name
Name is a byte array parsed as a string indicating the name of the feature level record.

In this case, after parsing `0x6d657461646174612e76657273696f6e` as a string, we get the value as `metadata.version`.

6d 65 74 61 64 61 74 61 2e 76 65 72 73 69 6f 6e
#### Feature Level
Feature Level is a 2-byte big-endian integer indicating the level of the feature.

In this case, the value is `0x14`, which is `20` in decimal. Indicating that, the `metadata.version` is at level `20`.

00 14
#### Tagged Fields Count
Tagged Field count is an unsigned variable size integer indicating the number of tagged fields.

In this case, the value is `0x00`, which is `0` in decimal. So, we can skip parsing the tagged fields.

00
  

### Headers Array Count
Header array count is an unsigned variable size integer indicating the number of headers present.

In this case, the value is `0x00`, which is `0` in decimal. So, we can skip parsing the headers.

00