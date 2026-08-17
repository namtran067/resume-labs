# Part 1 — Distributed Storage: HDFS → S3 → Delta → Iceberg → Hudi

> Nguồn đối chiếu: [HdfsDesign](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) · [docs.delta.io](https://docs.delta.io/latest/delta-batch.html) · [Iceberg Partitioning](https://iceberg.apache.org/docs/latest/partitioning/)

## 1. HDFS — nền tảng cần thuộc để hiểu mọi thứ sau này

### 1.1 Kiến trúc master/slave

```
                        ┌──────────────────────┐
      Client ──────────►│      NameNode        │  metadata trong RAM:
      (chỉ hỏi metadata) │  - namespace, tree   │  file → block → DataNode
                        │  - EditLog + FsImage │  (data KHÔNG bao giờ
                        └──────┬───────────────┘   chảy qua NameNode)
                    heartbeat │ blockreport
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
        ┌──────────┐    ┌──────────┐     ┌──────────┐
        │ DataNode │    │ DataNode │     │ DataNode │   lưu block 128MB
        │ blk-1    │    │ blk-1    │     │ blk-2    │   như file local
        └──────────┘    └──────────┘     └──────────┘
```

| Thành phần | Vai trò | Điểm cần nhớ |
|---|---|---|
| **NameNode** | Quản namespace + ánh xạ block→DataNode | Toàn bộ metadata nằm trong **RAM** → giới hạn số file/block của cluster |
| **DataNode** | Lưu/đọc/ghi block, báo Heartbeat + Blockreport | Không biết gì về file HDFS, chỉ biết block |
| **EditLog** | Transaction log mọi thay đổi metadata (append-only) | Ghi nhanh, replay khi khởi động |
| **FsImage** | Ảnh chụp toàn bộ namespace trên đĩa | **Checkpoint** = merge EditLog vào FsImage rồi truncate log (Secondary NameNode / Standby NN làm việc này) |
| **Safemode** | Trạng thái NN mới khởi động, chờ blockreport đủ % | Không replicate trong safemode |

### 1.2 Replication x3 + rack awareness (câu hỏi kinh điển)

Chính sách placement mặc định với replication factor = 3:

```
   Rack 1                 Rack 2
 ┌─────────┐           ┌───────────────┐
 │ writer↗ │           │ DN-B: replica │
 │ DN-A    │           │ DN-C: replica │  ← cùng rack xa
 │replica 1│           └───────────────┘
 └─────────┘
```

1. Replica 1: node của writer (hoặc random node **cùng rack** writer).
2. Replica 2: DataNode ở **rack khác** (đi qua switch — tốn bandwidth).
3. Replica 3: node **khác cùng rack xa** với replica 2.

**Trade-off**: chỉ 1 lần ghi xuyên rack (nhanh khi write) nhưng vẫn chịu được mất nguyên rack + đọc được từ 2 rack. DataNode chết (mất heartbeat > 10 phút) → NameNode **tự re-replicate** để đủ số bản.

### 1.3 Block 128MB và small files problem

- File = chuỗi block **128MB** (mặc định), write-once-read-many, dữ liệu pipeline DN1→DN2→DN3 khi ghi.
- **Small files problem**: mỗi file/block tốn ~150 bytes metadata **trong RAM NameNode** + 1 object khi list.
  - 10 triệu file 100KB = 1GB heap NameNode cho metadata, đọc thì seek-loạn (chết thiết kế "streaming access").
  - **Toa thuốc**: gộp file (HAR, SequenceFile, hoặc Parquet lớn), để table format compact (Delta `OPTIMIZE`, Iceberg `rewrite_data_files`), tránh over-partitioning.

**Câu chốt**: *"HDFS đánh đổi latency để lấy throughput: block to, write-once, compute đẩy tới gần data. NameNode là SPOF về metadata — giải quyết bằng HA với JournalNode (QJM)."*

## 2. S3 (object storage) — 5 khác biệt then chốt so với filesystem

| # | Filesystem (HDFS/NFS) | S3-style object storage | Hệ quả |
|---|---|---|---|
| 1 | Cây thư mục thật | **Flat key-space** (`s3://bucket/a/b/c.parquet` chỉ là prefix) | "Thư mục" là ảo giác của `LIST` |
| 2 | `rename` là operation metadata, O(1) | **Không có rename** = `COPY` toàn bộ object + `DELETE` | Commit protocol kiểu "write temp → rename" của Hadoop sập → cần S3A committer / magic committer |
| 3 | Sửa/append được file | **Immutable**: chỉ PUT (ghi đè toàn bộ) hoặc DELETE | Không append log truyền thống; Iceberg/Hudi phải thiết kế commit quanh điều này |
| 4 | Đổi nhiều file atomic trong 1 transaction | **Không atomic multi-object** (từ 2020 S3 có strong read-after-write consistency nhưng vẫn không lock/transaction) | Reader có thể thấy nửa dữ liệu → **gốc rễ của việc không có ACID trên S3** |
| 5 | Metadata op rẻ (open/list qua NameNode RAM) | **LIST chậm + trả theo 1000 keys/page**, request-rate limit theo prefix | Vài trăm nghìn file nhỏ = chết chi performance; metadata phải để catalogue ngoài |

**Chuỗi lập luận phỏng vấn (thuộc lòng):**

> S3 không có atomic multi-object rename → cách commit "viết file tạm rồi rename thư mục" vô dụng → hai writer đè nhau, reader đọc giữa chừng → **trên S3 thuần không có ACID** → bảng Parquet thô trên S3 ("data lake đời đầu") đầy lỗi Partial write, schema drift → **table format (Delta/Iceberg/Hudi) ra đời để thêm lớp transaction log trên object storage**.

MinIO / Cloudflare R2 = S3-compatible → đúng hệ quả trên (R2 còn có điểm thú vị: zero egress fee).

## 3. Delta Lake — ACID bằng `_delta_log`

### 3.1 Cơ chế

```
s3://lake/orders/
├── _delta_log/
│   ├── 00000000000000000000.json    ← mỗi commit = 1 file JSON
│   ├── 00000000000000000001.json      (add/remove file, schema, operation)
│   ├── 00000000000000000010.checkpoint.parquet  ← mỗi 10 commits
│   └── _last_checkpoint
├── part-0000-....snappy.parquet     ← data chính là Parquet
└── part-0001-....snappy.parquet
```

- **Mọi thay đổi bảng** (append/update/MERGE) = 1 transaction ghi vào log. Ghi log **atomic** (PUT file `N+1.json` chỉ thành công 1 lần — object storage guarantee unique key) → đây chính là "cây bút" của ACID trên S3.
- **Optimistic concurrency**: nhiều writer cùng commit → ai commit log trước thắng, thua thì retry (conflict nếu chạm cùng file).
- Đọc = đọc log → biết snapshot gồm những file nào → chỉ đọc các file đó.

### 3.2 Time travel & MERGE

```sql
-- Time travel: đọc snapshot cũ cho audit/debug/re-produce report
SELECT * FROM orders TIMESTAMP AS OF '2026-08-01';
SELECT * FROM orders VERSION AS OF 42;

-- MERGE = upsert (SCD-2, dedup, sync CDC)
MERGE INTO orders t
USING staging s ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Retention (mặc định, cần thuộc): `delta.logRetentionDuration = 30 days`; file data bị xóa **không tự động** — phải chạy `VACUUM` (`deletedFileRetentionDuration = 7 days`). `OPTIMIZE` + Z-order/liquid clustering để gộp file nhỏ.

**Câu chốt**: *"Delta = Parquet + một cuốn sổ cái JSON gọi là `_delta_log`; mọi operation đều phải ghi sổ trước, nên có atomicity, snapshot isolation, time travel và MERGE."*

## 4. Iceberg — metadata tree + hidden partitioning

### 4.1 Metadata tree (điểm khác biệt lớn nhất so với Delta)

```
Catalog (Hive/Glue/JDBC/REST-Polaris)          ← con trỏ atomic trỏ tới
   │
   ▼
metadata/00001-d96d....metadata.json           ← schema, partition spec, snapshots
   │  (mỗi snapshot trỏ tới)
   ▼
snapshots/00001-....avro  = manifest list      ← danh sách manifest
   ├── manifests/00001-m0.avro                 ← mỗi manifest: tracked data files
   │       └── data/part-00001.parquet         + column stats (min/max/count)
   └── manifests/00002-m1.avro
           └── data/part-00002.parquet
```

- Commit = swap con trỏ tới metadata.json mới **trong catalog ngoài** (đây là lúc atomic xảy ra). Vì thế **Iceberg bắt buộc có catalog** (Hive, Glue, JDBC, Nessie, REST/Polaris) — không "table by path" thuần như Delta.
- Manifest files chứa **statistics theo từng data file** → query planning bỏ qua hàng nghìn file không cần đọc mà không phải LIST storage.
- **Schema evolution đầy đủ**: add/drop/rename/reorder column an toàn nhờ field **ID** (không dựa tên/vị trí như Hive).

### 4.2 Hidden partitioning (tính năng đắt giá nhất)

```sql
CREATE TABLE events (
  event_id BIGINT,
  event_ts TIMESTAMP,     -- cột dữ liệu thật
  ...
) PARTITIONED BY (days(event_ts));   -- partition "ẩn"
```

| Transform | Ý nghĩa | Dùng khi |
|---|---|---|
| `days(ts)` / `months(ts)` / `hours(ts)` | Rời rạc hóa thời gian | Time-series, query theo ngày |
| `bucket(N, col)` | Hash vào N bucket | Tránh skew, chuẩn bị join 2 bảng cùng key |
| `truncate(L, col)` | Cắt value làm partition key | Giá trị dạng prefix/địa lý |
| `identity(col)` | Giữ nguyên value | Cột基数 nhỏ |

Điểm khác Hive/Delta: người query **chỉ filter trên `event_ts`**, Iceberg tự suy ra partition filter (trong Hive bạn phải tự tính `event_date` — quên là query scan toàn bảng). **Partition evolution**: đổi spec (`days` → `hours`) mà data cũ vẫn đọc được, mỗi manifest nhớ spec nó viết với spec nào.

## 5. Hudi — timeline & upsert-first

- Hadoop Update Delete → sinh ra từ **Uber** cho use case upsert gần real-time.
- **Timeline** (`.hoodie`): chuỗi **instant** (commit, deltacommit, clean, compaction...) là xương sống — mọi truy vấn "as of" một thời điểm.
- **Record-level index** (bloom / record index): biết chính xác file nào chứa key → upsert nhanh mà không cần scan toàn bảng (lợi thế so với Delta/Iceberg vốn phải dùng join để tìm file chứa row).

## 6. Bảng so sánh lớn — thuộc được là qua vòng Senior

| Tiêu chí | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Xuất xứ | Databricks | Netflix | Uber |
| Metadata | `_delta_log` trong thư mục bảng | **Metadata tree** + catalog ngoài | `.hoodie` timeline |
| Catalog | Metastore/path (log là source of truth) | **Bắt buộc catalog ngoài** (Hive/Glue/JDBC/REST) | Path + metastore |
| Partition | Cột partition rõ + liquid clustering | **Hidden partitioning + partition evolution** | Partition + clustering keys |
| Schema evolution | Tốt (column mapping) | **Tốt nhất** (field ID-based) | Tốt |
| Upsert/CDC | MERGE | MERGE | **First-class** (index, upsert API) |
| Điểm mạnh | Hệ sinh thái Spark/Databricks | Multi-engine (Spark, Flink, Trino, Hive, Snowflake...), chuẩn mở | Streaming/near-real-time, ingest lên bảng lớn |
| Điểm yếu | Gắn Spark (OSS), multi-writer cần DynamoDB lock | Catalog cần vận hành; đọc metadata hơi nặng lần đầu | API phức tạp hơn, cộng đồng hẹp hơn |

## 7. COW vs MOR (câu hỏi gần như chắc chắn xuất hiện)

```
COW (Copy-on-Write)                       MOR (Merge-on-Read)
┌─────────┐  update 5 rows                ┌─────────┐  update 5 rows
│ base    │───► REWRITE whole file        │ base    │───► append log/delta file
│ .parquet│     (file mới, chậm viết)     │ .parquet│     (viết nhanh)
└─────────┘                               └─────────┘
  Read: nhanh (1 file sạch)                 Read: base + log MERGE tại lúc đọc
                                            (hoặc compaction gộp về base)
```

| | COW | MOR |
|---|---|---|
| Write latency | Cao (rewrite nguyên file) | **Thấp** (append log) |
| Read latency | **Thấp** | Cao hơn nếu log dài → cần compaction |
| Phù hợp | Batch, read-heavy BI | Streaming, upsert liên tục (CDC) |
| Tương đương trong từng hệ | Delta MERGE, Iceberg mặc định | Hudi MOR table; Iceberg **position/equality delete files**; Delta **deletion vectors** |

**Câu chốt**: *"COW đổi công viết lấy công đọc; MOR ngược lại và trả giá bằng compaction — chọn theo tần suất ghi vs tần suất đọc. Thực tế nhiều đội chạy MOR + compaction theo lịch để cân bằng."*

## 8. Câu hỏi phỏng vấn thường gặp

1. NameNode chết thì sao? *(HA: active/standby + QJM shared edit log; FsImage + EditLog checkpoint)*
2. Vì sao có `_delta_log` rồi Iceberg còn cần cả metadata tree? *(scale metadata: Delta log lớn dần theo commit + stats theo file giúp planning; Iceberg tách manifest để pruning theo tầng)*
3. Hai job cùng MERGE một bảng Delta thì sao? *(optimistic concurrency — một bên ConflictException, retry)*
4. `bucket(N, col)` của Iceberg giải quyết vấn đề gì? *(skew + co-locate join key)*
5. Đã dùng cái nào thực tế? *(→ Part 5: lab Iceberg + Polaris + Trino trên MinIO; R2 data lake)*

*[← Part 0](part-0-overview.md) · [Part 2 →](part-2-batch-vs-stream.md)*
