# Part 3 — Storage Formats: Parquet, Avro, ORC

> Nguồn đối chiếu: [Parquet File Format](https://parquet.apache.org/docs/file-format/) · [Parquet Concepts](https://parquet.apache.org/docs/concepts/) · [Avro Specification](https://avro.apache.org/docs/1.11.3/specification/) · ORC spec (orc.apache.org)

## 1. Row vs Columnar — ví dụ "SUM(amount) WHERE city = 'HN'"

Bảng 100 triệu dòng, mỗi dòng 1KB gồm 20 cột (id, name, address, phone, created_at, city, amount, ...). Query chỉ chạm **2/20 cột**.

### Row-oriented (Avro, CSV)

```
Dòng 1: [id][name][address][phone][created_at][city][amount][...20 cột...]
Dòng 2: [id][name][address][phone][created_at][city][amount][...20 cột...]
...
→ Phải đọc 100% bytes (kể cả 18 cột không dùng) để lấy amount/city
→ Nén kém (mỗi dòng trộn nhiều kiểu dữ liệu)
```

### Columnar (Parquet, ORC)

```
Cột city:   [HN][SG][HN][HN][ĐN]...   → dictionary + RLE, nén cực tốt
Cột amount: [120][99][450][12][...]   → chỉ đọc đúng cột này
→ IO ≈ 2/20 ≈ 10% dữ liệu; min/max của từng block giúp bỏ qua nữa
```

| Khía cạnh | Row | Columnar |
|---|---|---|
| `SELECT *` lấy 1 dòng | **Nhanh** (liền mạch) | Chậm (ghép từ nhiều cột) |
| Aggregate/scan ít cột | Chậm (đọc thừa) | **Nhanh (column pruning)** |
| Filter `WHERE` | Scan hết | **Predicate pushdown qua min/max** |
| Nén | Kém | **Tốt** (dữ liệu đồng nhất theo cột) |
| Ghi append liên tục | **Tốt** (streaming) | Kém (buffer theo batch) |
| Use case | OLTP, message bus | OLAP, data lake/warehouse |

**Mnemonic**: *transactional = row, analytical = column.*

## 2. Parquet — layout file chính xác theo spec

```
┌────────────────────────────────────────────────────────┐
│ 4-byte magic: "PAR1"                                   │
│ <Column 1 Chunk 1> <Column 2 Chunk 1> ... <Column N 1> │ ← Row Group 1
│ <Column 1 Chunk 2> <Column 2 Chunk 2> ... <Column N 2> │ ← Row Group 2
│          ...          ...          ...                  │
│ File Metadata (footer, Thrift)                          │
│ 4-byte: length của metadata (little endian)             │
│ 4-byte magic: "PAR1"                                    │
└────────────────────────────────────────────────────────┘
```

(theo parquet.apache.org/docs/file-format — metadata viết **cuối file** để single-pass writing; reader đọc footer trước)

| Khái niệm | Kích thước điển hình | Ghi chú |
|---|---|---|
| **Row group** | ~128MB (khớp block HDFS) | Đơn vị đọc song song / horizontal split |
| **Column chunk** | 1 phần của 1 cột trong 1 row group | Đơn vị nén riêng |
| **Page** | ~1MB | Đơn vị encoding (dictionary, RLE) |
| **Footer** | — | Schema + offsets + **statistics min/max/count/null_count mỗi column chunk** |

**Predicate pushdown** (vẽ được là ghi điểm):

```
WHERE city = 'HN' AND amount > 0
1. Đọc footer:  city chunk #3 có min='ĐN', max='SG'  → bỏ qua (HN nằm ngoài range)
2. Dictionary lookup: 'HN' → giá trị mã hóa, RLE decode nhanh
3. Chỉ decompress pages của amount tương ứng
→ Có thể bỏ qua 90%+ bytes mà không cần đọc data
```

Nén: Snappy (nhanh, mặc định Spark), ZSTD (tỉ lệ tốt, ngày càng phổ biến), GZIP (chậm). Nested data: Dremel encoding (repetition/definition levels).

## 3. Avro — schema evolution + Kafka single-object encoding

### 3.1 Đặc tính

- **Row-based**, schema **JSON** tách rời data; binary encoding **không chứa tên field** → nhỏ, nhanh.
- File container: magic `Obj\x01` + metadata (kể cả `avro.schema`) + sync marker, chia block nén được (deflate/snappy/zstd).

### 3.2 Schema evolution (lý do Avro "sống" ở ingestion)

Writer schema ≠ Reader schema vẫn đọc được — **Schema Resolution** theo spec:

| Thay đổi | Kết quả |
|---|---|
| Reader có field mới **có default** | Điền default |
| Reader có field mới **không default** | Lỗi |
| Writer có field reader không có | Bỏ qua (field khớp theo **tên**, không theo vị trí) |
| Promotion | `int → long → float → double` đọc được, ngược lại không |
| Đổi tên | Dùng `aliases` |

→ So sánh: Parquet schema evolution yếu hơn (add column được, rename/đổi kiểu rất hạn chế) — vì thế **Avro đứng ở cửa, Parquet ở trong bàn**.

### 3.3 Single-object encoding cho Kafka (spec mục này hay bị hỏi)

```
┌──────────┬───────────────────────────┬──────────────────────┐
│ C3 01    │ CRC-64-AVRO fingerprint   │ Avro binary payload  │
│ (2 byte) │ của schema (8 byte LE)    │                      │
└──────────┴───────────────────────────┴──────────────────────┘
```

- Spec viết rõ use case: *"storing Avro records for several weeks in an Apache Kafka topic"* — record tự mang dấu vân tay schema → consumer tra Schema Registry theo fingerprint, xử lý đúng kể cả khi schema đổi giữa chừng.
- Biến thể phổ biến: **Confluent Wire Format** = magic byte `0` + 4-byte schema-ID + payload.

## 4. ORC (Optimized Row Columnar)

- Từng là "Parquet của hệ Hive/Presto": **stripe** (~64–256MB) là đơn vị đọc; mỗi stripe có **index** (min/max mỗi 10k dòng), footer, dictionary/RLE encoding + bloom filter.
- Hỗ trợ **ACID transactional tables** đầy đủ trong Hive (base + delta files) trước khi Delta/Iceberg phổ biến.
- Hiện tại: chọn khi gắn hệ sinh thái Hive/Trino/Presto; cộng đồng còn lại thiên về Parquet. Nén tốt hơn Parquet chút (lightweight encode mặc định), nhưng tooling/interop hẹp hơn.

## 5. Bảng so sánh 3 format

| | Avro | Parquet | ORC |
|---|---|---|---|
| Layout | Row | Columnar | Columnar |
| Schema | JSON, tách rời, evolution mạnh | Trong footer, evolution hạn chế | Trong footer |
| Tốt cho | Streaming ingest, message bus (Kafka), RPC | OLAP đa năng, multi-engine | Hive/Trino-centric |
| Nén | Trung bình | Tốt (snappy/zstd) | Rất tốt |
| Split được | Có (sync marker) | Có (row group) | Có (stripe) |
| Ngôn ngữ mô tả | Spec độc lập, nhiều SDK | Thrift metadata | Protobuf metadata |

## 6. Quy tắc vàng: "Avro at the door, Parquet at the table"

```
           CỬA (ingestion)                    BÀN (analytics)
┌────────────────────────────┐    ┌─────────────────────────────────┐
│ Kafka topic: Avro          │    │ Iceberg/Delta table: Parquet    │
│ + Schema Registry          │───►│ + partition + stats ở footer    │
│ • append nhanh             │comp│ • scan ít cột, predicate pushdown│
│ • evolution an toàn        │act │ • nén tốt, rẻ lưu dài hạn       │
│ • replay được nhiều schema │    │ • BI/ML đọc trực tiếp           │
└────────────────────────────┘    └─────────────────────────────────┘
```

Lý do không dùng Parquet ngay ở cửa: columnar khó append từng dòng nhỏ (phải buffer batch); Avro stream-friendly + đổi schema liên tục an toàn. Lý do không giữ Avro ở tầng phân tích: query analytic trên row format đọc thừa 10x. Map sang medallion: **Bronze = Avro/JSON thô, Silver/Gold = Parquet**.

## 7. Câu hỏi phỏng vấn thường gặp

1. Vì sao `SELECT *` trên Parquet chậm hơn Avro? *(Ghép N cột từ N chunk + decompress nhiều page)*
2. Row group to hay nhỏ tốt hơn? *(To = ít metadata, nén tốt, scan nhanh; nhỏ = selective query + parallelism mịn — 128MB là điểm cân bằng lịch sử theo HDFS block)*
3. Đổi schema Kafka topic đang chạy? *(Registry + backward compatibility, consumer dùng single-object/confluent format tra đúng schema cũ)*
4. CSV + gz thay Parquet được không? *(Được nhưng mất: column pruning, predicate pushdown, stats, schema — chi phí scan và tiền S3 scan (Athena/Snowflake) đội lên nhiều lần)*

*[← Part 2](part-2-batch-vs-stream.md) · [Part 4 →](part-4-spark-optimization.md)*
