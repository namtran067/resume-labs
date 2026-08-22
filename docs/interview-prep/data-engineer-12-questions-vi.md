# Dự đoán câu hỏi & Khung trả lời phỏng vấn

**Ứng viên:** Trần Nhật Nam — Product Data Engineer
**Vị trí mục tiêu (giả định theo CV):** Senior / Product Data Engineer — product company hoặc scale-up data-driven
**Ghi chú:** Khi có JD cụ thể, map lại cột trái của từng câu với trách nhiệm trong JD và thay câu hỏi ngược bằng câu chạm đúng pain point của công ty đó.

**Nguyên tắc chung trước khi vào đề:**
- Mỗi câu trả lời chỉ nên nói 90–120 giây. STAR không phải là kể chuyện dài — S và T gộp lại 2 câu, A là phần chính, R kết bằng số.
- Số liệu phải nói được cách đo. Mọi con số trong CV (90%, 60%, 3x, 97%) đều phải có "backstory" 1 câu: đo thế nào, baseline là gì.
- Không bao giờ claim thứ không defend được ở 2 tầng câu hỏi sâu.

---

## Phần A — 12 câu hỏi dự đoán & trả lời mẫu STAR

### Câu 1 (Kiến trúc — gần như chắc chắn bị hỏi)
**"Tell me about a data platform you designed end-to-end. Why those tools?"**

Nguyên tắc: interviewer không chấm công nghệ bạn dùng — họ chấm **cách bạn ra quyết định**. Mọi tool phải đi kèm "tôi cân nhắc X, loại Y, vì Z". Nói S+T trong 20 giây, dành 60 giây cho A, 10 giây cho R.

#### Script nói (~90 giây)

**S (~15s):** *"Jemmia là chuỗi bán lẻ trang sức. Khi tôi vào, dữ liệu nằm rải rác ở hơn 12 nguồn — Facebook/TikTok Ads, Haravan, ERP, tổng đài Stringee, máy quét RFID, và hàng chục file Excel. Hệ quả: hai phòng đưa ra hai con số doanh thu khác nhau trong cùng một cuộc họp BOD, và quyết định mua hàng trị giá hàng chục nghìn USD được đưa ra trên dữ liệu cũ."*

**T (~10s):** *"Tôi được giao xây một nền tảng dữ liệu tập trung — với hai ràng buộc không thương lượng: tôi về bản chất là đội data một người, và ngân sách SME. Nên yêu cầu thiết kế số một không phải performance — mà là platform phải tự vận hành, tự documentation, và license bằng 0."*

**A (~55s):** *"Tôi đưa việc chọn công cụ về sau ba quyết định chính — và mỗi cái tôi đều cân nhắc phương án bỏ đi:

Thứ nhất, orchestration. Tôi đã dùng Airflow ở công ty trước — nó là chuẩn ngành. Nhưng tôi chọn **Dagster**, vì một lý do rất cụ thể: với software-defined assets, mỗi asset tự khai báo lineage, freshness và partition của chính nó — asset graph chính là documentation sống. Với Airflow, tôi sẽ viết DAG rồi maintain tài liệu lineage riêng, và hai thứ đó chắc chắn sẽ lệch nhau. Với đội một người, công cụ tự documenting thắng công cụ mạnh hơn nhưng đòi documentation tay.

Thứ hai, ingestion — tôi không dùng một cây búa cho mọi việc. **Airbyte** cho nguồn có connector sẵn, **dlt** cho nguồn cần schema contract chặt như ERP và tổng đài, **n8n** cho automation nhẹ quanh app.

Thứ ba, warehouse. Tôi cân nhắc BigQuery — nhưng làm toán đơn giản: 500K rows/ngày, tức khoảng 180M rows/năm. Postgres với partitioning xử lý thoải mái với chi phí gần bằng không. Quan trọng hơn, tôi giữ toàn bộ transformation trong **dbt** để không khóa vendor — cùng codebase đó tôi đã chạy trên BigQuery trong project cá nhân. Nếu quy mô tăng gấp mười, chuyển warehouse là đổi config, không phải viết lại."*

**R (~10s):** *"Platform chạy production hơn hai năm: 12+ nguồn, 150 raw tables, nửa triệu rows mỗi ngày, chi phí vận hành ETL giảm 90% so với stack Zapier cũ, license bằng 0. Và dấu hiệu tôi tự hào nhất: phòng Finance bắt đầu đối chiếu sổ kế toán theo số của warehouse — đó là lúc tôi biết họ tin nó."*

> **Câu nói quyền năng:** *"Tôi thiết kế cho ràng buộc khắc nghiệt nhất của hệ thống — đội một người. Mỗi quyết định công nghệ đều phải trả lời được một câu hỏi: platform này có sống sót khi tôi đi nghỉ phép không?"*
> **Metric:** -90% ETL cost, 12+ nguồn, 500K+ rows/day, 2+ năm production, license = 0.

#### Đạn dự phòng — follow-up chắc chắn sẽ tới

| Follow-up | Trả lời |
|---|---|
| **"Why not Airflow?"** | Tôi đã dùng Airflow ở AVLI (orchestrate cả pipeline scraping + warehouse 15 bảng) — tôi biết nó. Lý do chọn Dagster: SDA cho declarative lineage/freshness/partitions; asset graph là docs sống; backfill theo partition là first-class; failure localize đúng asset. Trade-off tôi chấp nhận: ecosystem Airflow lớn hơn, dễ tuyển người hơn — nhưng với đội 1 người, giá trị của ecosystem thấp hơn giá trị của self-documentation. |
| **"Why Postgres, not BigQuery/Snowflake?"** | Quy mô 500K rows/ngày không cần cloud warehouse — Postgres + partitioning + indexing là đủ, rẻ, và ops đơn giản. Điểm mấu chốt: kiến trúc không khóa vendor. dbt trừu tượng hóa warehouse — tôi đã chứng minh bằng cách chạy cùng kiểu project trên BigQuery (CryptoStream) và StarRocks (side project). Upsize là quyết định kinh doanh, không phải migrate codebase. |
| **"Why no Kafka/streaming?"** | Decisions ở Jemmia theo nhịp ngày/tuần — batch SLA vài tiếng là đủ. Nguồn duy nhất gần real-time là RFID, và nó đã event-driven qua Lambda webhook rồi — không cần broker. Tôi biết chỗ đứng của Kafka: khi nhiều consumer cần cùng một event stream với latency dưới một phút. Ở quy mô đó, ops burden của Kafka không mang lại giá trị tương xứng. |
| **"What breaks at 10x scale?"** | Thứ nhất: Postgres — đơn node, không columnar; kế hoạch là đẩy analytical workload sang BigQuery/StarRocks, chỉ đổi target trong dbt. Thứ hai: Dagster đơn node — cần horizontal scale hoặc chia theo domain. Thứ ba: ingestion dlt scale được theo nguồn. Tôi đã prototype kiến trúc kế tiếp: Mini Lakehouse dùng Iceberg + Spark + Trino — tức tôi không chỉ biết platform hiện tại chết ở đâu, tôi đã build thử bản kế nhiệm. |
| **"How do you recover a bad load?"** | Raw layer immutable — không bao giờ transform in-place, nên mọi thứ downstream replay được từ raw. Dagster partition backfill cho phép load lại đúng khoảng thời gian lỗi, không chạy lại toàn pipeline. |

**Câu hỏi ngược:** *"Nếu được xây lại data platform của team từ đầu hôm nay với những gì anh/chị biết bây giờ, anh/chị sẽ giữ gì và bỏ gì?"* — câu này khiến interviewer phải tự phê bình stack của họ, và câu trả lời của họ tiết lộ chính xác pain point mà bạn sẽ được thuê để giải.

---

### Câu 2 (Data modeling — kỹ thuật)
**"Walk me through how you design dbt models. Raw → marts, why medallion?"**

- **S:** Cùng bối cảnh Jemmia — 150 raw tables, yêu cầu báo cáo từ 4 phòng ban với節奏 thay đổi liên tục.
- **T:** Thiết kế warehouse sao cho thêm 1 báo cáo mới = việc của giờ, không phải của tuần; và dữ liệu vào marts phải được test trước khi business nhìn thấy.
- **A:** Star schema Kimball làm nền. Pipeline 4 lớp: raw (immutable, không đụng vào) → staging (rename, cast, 1:1 với nguồn) → intermediate (business logic) → marts (star schema, mỗi fact/dim phục vụ câu hỏi business). 200+ dbt models với tests 4 loại: schema, not-null, relationships, freshness. PII masking ở staging, lineage khai báo qua OpenMetadata.
- **R:** Thời gian phát triển và bảo trì giảm 90% — thêm metric mới chỉ là thêm 1 model và test.

> **Câu nói quyền năng:** *"Mỗi model trong marts phải trả lời được một câu hỏi business bằng một câu — nếu không, nó không có quyền tồn tại."*
> **Metric:** 200+ models, -90% thời gian dev/maintenance.

**Câu hỏi ngược:** *"Team mình đang theo convention nào — Kimball, OBT, wide tables — và điều gì khiến team chọn hướng đó?"*

---

### Câu 1 xong → chú ý: nếu interviewer hỏi sâu **"Why PostgreSQL as a warehouse, not BigQuery/Snowflake?"** → trả lời: quy mô 500K rows/ngày + ngân sách SME, Postgres + partitioning + đúng indexing là đủ và rẻ; quan trọng hơn là kiến trúc không khóa vendor — cùng codebase dbt tôi đã chạy trên BigQuery trong project CryptoStream, nên khi quy mô tăng, migration là quyết định kỹ thuật chứ không phải viết lại.

---

### Câu 3 (Vấn đề phức tạp nhất — hardware/IoT)
**"What's the most complex data problem you've solved?"**

- **S:** Jemmia dùng máy quét RFID vật lý cho kho; mỗi lần quét đẩy ~1.000 records / 3MB, 20 quét/ngày. Workflow cũ chạy qua Zapier — brittle, throttle, mất sự kiện.
- **T:** Tích hợp thiết bị hardware với hệ thống e-commerce thành pipeline ổn định, đủ chuẩn cho phân tích time-series tồn kho và chuỗi cung ứng.
- **A:** Thiết kế kiến trúc event-driven trên AWS Lambda (deploy AWS SAM): scanner → webhook → Lambda xử lý → warehouse. Thiết kế idempotent (event trùng không nhân bản dữ liệu), schema tối ưu cho time-series, xử lý backpressure khi quét trùng giờ.
- **R:** Pipeline chạy ổn định, thay thế hoàn toàn Zapier, mở khóa được phân tích tồn kho theo thời gian thực mà trước đây không thể.

> **Câu nói quyền năng / Metric:** *"~1.000 bản ghi mỗi lần quét, 20 lần quét mỗi ngày — và kể từ khi chuyển sang event-driven, chúng tôi không bỏ lỡ một sự kiện nào. Zapier cũ thì bỏ lỡ hàng tuần."*

**Câu hỏi ngược:** *"Team mình có data từ thiết bị/edge không? Mình xử lý độ tin cậy của ingestion từ nguồn ngoài kiểm soát như thế nào?"*

---

### Câu 4 (Xung đột stakeholder — behavioral)
**"Tell me about a time you disagreed with a stakeholder about data."**

- **S:** Tại Jemmia, Sales và Finance mỗi nơi báo "doanh thu" một số — gross, net, có/thuế, từng thời điểm cut-off. Mỗi lần họp BOD là một cuộc tranh cãi về số liệu thay vì quyết định.
- **T:** Làm một định nghĩa chuẩn được cả 4 phòng và BOD chấp nhận — không thể "đi họp với hai con số".
- **A:** Tôi nhận ra gốc vấn đề không phải dữ liệu mà là định nghĩa. Tổ chức working session với Sales, Marketing, Finance, BOD: mỗi metric được định nghĩa một lần, ghi thành semantic layer có governance, code hóa trong dbt/marts làm single source of truth. Sau đó chuyển dashboard sang Metabase self-service với metric đã chuẩn hóa — ai cũng tự kéo số, nhưng từ một định nghĩa duy nhất.
- **R:** 100% key metrics có một định nghĩa duy nhất; quyết định dựa trên dữ liệu nhanh hơn tới 3x; các cuộc tranh cãi "số của ai đúng" biến mất.

> **Câu nói quyền năng:** *"Mọi tranh cãi về số liệu thực chất là tranh cãi về định nghĩa. Tôi giải quyết nó một lần — ở semantic layer, không phải trong phòng họp."*
> **Metric:** 100% coverage key metrics, decisions 3x faster.

**Câu hỏi ngược:** *"Data team ở công ty mình được xem là cost center hay profit driver — và anh/chị thấy điều gì cần thay đổi nhất trong data culture hiện tại?"*

---

### Câu 5 (Data quality / incident)
**"A dashboard shows wrong numbers in a BOD meeting. What do you do?"**

- **S:** Là rủi ro tồn tại thật của tôi: dashboard BOD dùng hàng ngày; một silent failure ở nguồn bên ngoài (API đổi schema, dữ liệu trống) có thể tạo số sai mà không ai báo lỗi.
- **T:** Thiết kế để điều đó không bao giờ xảy ra im lặng — sai thì phải sai sớm và sai ồn ào.
- **A:** Ba lớp phòng thủ: (1) dbt tests ở từng lớp — schema, not-null, relationships, freshness — dữ liệu lỗi dừng trước khi tới marts; (2) real-time anomaly alerting qua Sentry cho pipeline và ứng dụng; (3) reconciliation định kỳ số warehouse vs nguồn. Khi có incident: khóa dashboard metric nghi ngờ → root cause → communicate minh bạch với stakeholder → post-mortem và thêm test chống tái phát.
- **R:** 100% key metrics được phủ test; các sự cố từ "phát hiện trong họp" thành "phát hiện trước giờ làm việc".

> **Câu nói quyền năng:** *"Tôi thiết kế pipeline để khi nó fail, nó fail lúc 2 giờ chiều với alert — chứ không phải fail im lặng rồi được phát hiện trong phòng họp BOD."*

**Câu hỏi ngược:** *"Observability stack của team mình như thế nào — team dùng dbt tests, Elementary, Monte Carlo hay tự xây? SLA cho data của mình được định nghĩa ra sao?"*

---

### Câu 6 (Migration zero-downtime — kỹ thuật + quản trị)
**"Tell me about a migration you led. How did you avoid downtime?"**

- **S:** Jemmia chạy Odoo legacy; dữ liệu master sản phẩm 10GB+, 8.000+ SKU, đang vận hành bán hàng hàng ngày — không có "cửa sổ dừng hệ thống".
- **T:** Chuyển sang ERPNext + tự xây PIM trên NocoDB (40+ bảng) trên data lake Cloudflare R2 — mà không gián đoạn bán hàng.
- **A:** Chiến lược backfill song song: đồng bộ dữ liệu lịch sử trước, giữ hai hệ thống chạy cùng lúc, CDC (change data capture) bắt mọi thay đổi mới trong lúc chuyển đổi, validation bằng checksum/report đối chiếu từng phân khúc dữ liệu trước khi cutover từng nhóm user. Song song đó: training cho từng phòng, thu feedback từ nhóm pilot trước khi mở rộng.
- **R:** Zero-downtime cho 8.000+ SKU; 97% nhân viên áp dụng hệ thống mới; thao tác thủ công liên phòng giảm 40%.

> **Câu nói quyền năng:** *"Migration thành công không phải khi dữ liệu chuyển hết qua hệ thống mới — mà khi 97% nhân viên tự nguyện dùng nó."*
> **Metric:** 97% adoption, -40% manual ops, 8.000+ SKU.

**Câu hỏi ngược:** *"Team mình có đang có kế hoạch migration nào (stack, warehouse, ERP) không? Cách team mình quản lý change với user бизнес-side ra sao?"*

---

### Câu 7 (SQL / incremental — kỹ thuật sâu, có thể live coding)
**"How do you handle late-arriving data? Design an incremental strategy."**

- **S:** Dữ liệu Haravan/ads có ngày về muộn hoặc chỉnh sửa retroactive — full refresh tốn kém và chậm, incremental thuần làm số liệu lệch.
- **T:** Incremental mà vẫn correct với dữ liệu đến muộn và sửa ngược.
- **A:** Với dbt incremental: dùng `unique_key` + merge thay vì append; lookback window đủ rộng (ví dụ load lại 3–7 ngày gần nhất thay vì 1 ngày) để bắt late-arriving; dedup bằng window function `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY updated_at DESC)` lấy bản mới nhất; những nguồn hay sửa ngược thì đánh dấu partition re-process. Nguyên tắc: incremental là quyết định về correctness trước, hiệu năng sau — và phải có lối thoát full-refresh một lệnh.
- **R:** Pipelines chạy Incremental ổn định, thời gian refresh từ giờ xuống phút, không còn số lệch do late data.

> **Câu nói quyền năng:** *"Incremental không phải một tối ưu hóa — nó là một cam kết về tính đúng đắn. Tôi luôn thiết kế như thể dữ liệu nhất định sẽ đến muộn, vì nó sẽ đến muộn."*

**Câu hỏi ngược:** *"Data SLA của team mình tính từ sự kiện xảy ra đến khi có mặt trong dashboard là bao lâu, và có nguồn nào team đang phải đánh vật với late-arriving data không?"*

---

### Câu 8 (Thất bại / sai lầm — behavioral, gần như chắc chắn có)
**"Tell me about a time you got something wrong."**

- **S:** Ở AVLI, tôi build scraping workers cho batdongsan.com và 7+ site bất động sản. Một ngày website đổi HTML structure — scraper vẫn "chạy xanh", không lỗi, nhưng hút về trường sai/im lặng thiếu dữ liệu vào warehouse 300K+ bản ghi.
- **T:** Phát hiện, cứu dữ liệu, và làm sao để loại lỗi "im lặng" này không tái diễn.
- **A:** Phát hiện qua đối chiếu row count/số liệu bất thường. Khôi phục bằng cách re-crawl phần thiếu. Fix gốc: thêm schema tests + not-null tests cho mọi bảng staging (schema đổi → test fail ngay, không chờ đến dashboard), thêm monitoring volume theo nguồn (thiểu số bất thường → alert), và coi mọi nguồn ngoài là hostile.
- **R:** Sau fix, các thay đổi schema phía nguồn đều bị bắt trong vòng một chu kỳ load — trước khi chạm tới analyst.

> **Câu nói quyền năng:** *"Sau sự cố đó tôi rút ra một quy tắc: mọi nguồn dữ liệu bên ngoài đều là hostile — nó sẽ đổi mà không báo trước, và việc của tôi là làm cho sự đổi đó ồn ào."*

**Câu hỏi ngược:** *"Team mình có văn hóa post-mortem không — blameless chứ ạ? Ví dụ gần nhất team học được gì từ một incident?"*

---

### Câu 9 (GenAI — điểm khác biệt của CV này)
**"You built an NL-query bot. How did you make it secure?"**

- **S:** Lãnh đạo muốn "hỏi dữ liệu bằng tiếng Việt trên điện thoại" nhưng warehouse chứa dữ liệu bán hàng, PII khách hàng — không thể bung database ra cho LLM.
- **T:** Build **Metabot** — agent Telegram/Discord cho câu hỏi natural-language mà không lộ database.
- **A:** Nguyên tắc: LLM không bao giờ chạm SQL trực tiếp. Nó nói chuyện qua Metabase API như một user có permission — chỉ truy cập collections/dashboard đã whitelist; RBAC ở tầng BI, không ở prompt. Context caching bằng Redis để hội thoại nhiều lượt mà không gửi lại toàn bộ ngữ cảnh. Next.js/React cho interface. Song song đó build **Salesaya** — AI sales assistant trên warehouse cho đội bán hàng (RAG + LLM APIs).
- **R:** BOD và trưởng phòng tự hỏi đáp dữ liệu không cần chờ analyst; Salesaya cắt 60% thời gian tư vấn (đo bằng thời gian trung bình mỗi buổi tư vấn trước/sau khi dùng).

> **Câu nói quyền năng:** *"LLM trong hệ thống của tôi không có quyền hạn gì hơn một nhân viên bình thường — nó đi qua cùng một API, cùng một RBAC. Security ở tầng permission, không ở tầng prompt."*
> **Metric:** -60% consultation time (Salesaya).

**Câu hỏi ngược:** *"Team mình có AI roadmap cho data products không — và anh/chị thấy ranh giới đúng giữa 'demo AI hay' và 'AI đáng tiền' nằm ở đâu?"*

---

### Câu 10 (Ưu tiên — situational, đặc thù làm 1 mình)
**"Four departments need you at once. How do you prioritize?"**

- **S:** Là data partner chính của Sales, Marketing, Finance và BOD — 4 nơi, mỗi nơi coi yêu cầu của mình là khẩn cấp nhất.
- **T:** Không thể làm hết — phải có hệ thống ưu tiên và phải giảm cầu thay vì chỉ tăng cung.
- **A:** Ba việc: (1) mọi yêu cầu qua một intake đơn giản, chấm điểm theo impact kinh doanh × effort; (2) giảm cầu bằng self-service — Metabase với metric chuẩn hóa, đào tạo từng phòng tự phục vụ 80% câu hỏi thường ngày; (3) dạy câu cá — data-literacy workshops cho BOD và trưởng phòng. Việc còn lại cho tôi là những gì chỉ tôi làm được: platform, modeling, governance.
- **R:** Quyết định dựa dữ liệu nhanh tới 3x; tôi không còn là bottleneck; tỷ lệ yêu cầu "ad-hoc" giảm mạnh vì người dùng tự phục vụ được.

> **Câu nói quyền năng:** *"Việc của tôi không phải trả lời mọi câu hỏi — mà là xây hệ thống để người khác tự trả lời 80% câu hỏi của chính họ."*

**Câu hỏi ngược:** *"Data team mình nhận request theo quy trình nào — và tỷ lệ request ad-hoc vs project dài hạn hiện như thế nào?"*

---

### Câu 11 (Cost optimization)
**"Tell me about a time you saved the company money."**

- **S:** Stack cũ: Zapier cho ETL (chi phí tăng theo volume, brittle) + license Looker cho BI — bài toán kinh điển của SME: chi phí công nghệ lớn hơn giá trị nó tạo.
- **T:** Cắt chi phí mà không cắt năng lực phân tích.
- **A:** Hai nhát: (1) thay Zapier bằng self-hosted open-source — Dagster + dlt + Airbyte, vận hành qua Dokploy trên hạ tầng tự chủ, thêm NAS có replication off-site cho DR; (2) retirе license Looker, chuyển sang Metabase self-hosted với semantic layer chuẩn hóa — không mất năng lực, người dùng còn tự phục vụ được nhiều hơn. Không phải "cắt để tiết kiệm" mà là "chuyển tiền từ license sang năng lực tự chủ".
- **R:** Chi phí vận hành ETL giảm 90%; loại bỏ hoàn toàn license BI; năng lực phân tăng lên (self-service + 3x tốc độ quyết định).

> **Câu nói quyền năng:** *"Tôi không cắt chi phí bằng cách cắt tính năng — tôi cắt bằng cách ngừng trả license cho những thứ open-source làm được tốt hơn."*
> **Metric:** -90% ETL cost, BI licensing = 0.

**Câu hỏi ngược:** *"Hạ tầng data của team mình đang là self-hosted hay managed — và team cân nhắc trade-off cost vs ops-effort đó ra sao?"*

---

### Câu 12 (Học tập & định hướng — đóng màn)
**"You studied Data Science. Why data engineering? And how do you learn new tech?"**

- **S:** Tốt nghiệp Data Science ở UIT — nhưng công việc đầu tiên cho tôi thấy: model chỉ giá trị khi có data đúng, đúng giờ, đúng ngữ cảnh. Bottleneck của hầu hết công ty không phải ML — mà là nền tảng dữ liệu.
- **T:** Chọn nghề nền tảng; và với tốc độ công nghệ data đổi mỗi năm, phải có cách học bền.
- **A:** Tôi học bằng cách build. Mỗi công nghệ mới, tôi làm một project chạy được và public: Mini Lakehouse (Iceberg + Spark + Trino + Polaris trên MinIO) để hiểu lakehouse đa engine thực sự hoạt động; CryptoStream (Airflow + dbt + BigQuery trên GKE Helm) để thực hành cloud-native; open-source Docker templates cho Airflow/Airbyte/StarRocks để đóng góp lại cộng đồng. Ở AISIA tôi cũng từng bước vào ML từ mặt dữ liệu: label 80+ giờ tiếng nói, giúp giảm WER ~12% cho Wav2Vec 2.0.
- **R:** CV của tôi không có công nghệ nào tôi không chạy được bằng tay — mỗi dòng đều có repo đằng sau.

> **Câu nói quyền năng:** *"Tôi học bằng cách build, không phải bằng cách đọc — nên mọi công nghệ trong CV của tôi đều có một repo chạy được đứng sau nó."*
> **Metric:** 9 public repos/projects; ASR WER -12%.

**Câu hỏi ngược:** *"Tech radar của team mình như thế nào — team tiếp nhận công nghệ mới theo quy trình gì, và anh/chị mong người mới tạo impact đầu tiên trong bao lâu?"*

---

## Phần B — Điểm yếu interviewer có thể xoáy & cách xử lý khôn ngoan

| # | Điểm yếu trong CV | Interviewer sẽ hỏi | Cách xử lý |
|---|---|---|---|
| 1 | **Quy mô dữ liệu SME** (500K rows/day, PostgreSQL) — nhỏ so với big-tech | "Your scale is quite small, we process millions of rows/hour" | Đừng phòng thủ. Thừa nhận quy mô, rồi lật: "Đúng, và vì vậy tôi sở hữu toàn bộ chuỗi end-to-end — từ đàm phán yêu cầu với BOD tới paging chính mình khi pipeline chết. Mỗi quyết định kiến trúc là của tôi và tôi chịu trách nhiệm kết quả." + chỉ ra Mini Lakehouse/Iceberg là bằng chứng đang chủ động chuẩn bị cho scale lớn hơn. Quy mô nhỏ, ownership toàn phần. |
| 2 | **Tiếng Anh** (TOEIC 725) — nếu phỏng vấn công ty nước ngoài | Round tiếng Anh, presentation | Chủ động trước: "My reading and writing are strong — all my documentation is in English; speaking I'm actively improving." Nếu nghiêm túc với công ty quốc tế: luyện 5 câu chuyện STAR bằng tiếng Anh trước phỏng vấn, nói chậm, đúng số liệu. Đừng khai trừ khi được hỏi trực tiếp. |
| 3 | **GPA 7.9/10** — không xuất sắc | "Why is your GPA modest?" | Một câu, không biện hộ nhiều: "I spent my university years building — scraping projects, the AISIA research lab, freelance work. Four years of production experience since then is a better predictor than a number from 2023." Dừng lại, không dài dòng. |
| 4 | **Timeline chồng lấn** — BabyBet (Jul 2023–Mar 2024) chồng lên Jemmia (Sep 2023–nay); AISIA cũng chồng AVLI | "You had two jobs at once?" | Minh bạch ngay từ đầu: BabyBet là contract part-time (nghiên cứu tính khả thi một venture mobility), số giờ/commit rõ ràng, không xung đột. Trả lời ngắn, tự tin, có sẵn chi tiết scope — sự minh bạch biến điểm ngờ vực thành điểm cộng đa nhiệm. |
| 5 | **Kafka trong Skills nhưng không có project nào dùng Kafka** | "Tell me about your Kafka experience" (bẫy kinh điển) | Trung thực có kiểm soát: "I understand Kafka's model and have worked with event-driven patterns via webhooks/Lambda, but I haven't run Kafka in production — my volume didn't justify it. I know when I'd reach for it." Nếu giữ Kafka trong skills: ôn nhanh toàn bộ khái niệm + lỗi hay gặp + quy tắc vàng ở **Phần D** — và luôn đóng bằng câu "em chọn không dùng Kafka vì chưa chạm ngưỡng nó trả tiền: consumer thứ hai, burst, replay". **Khuyến nghị mạnh: cân nhắc bỏ Kafka khỏi skills.tex** — chỉ giữ những gì defend được 2 tầng câu hỏi. |
| 6 | **"Product Data Engineer"** — title không chuẩn, khó so sánh | "Is that a real senior title? What level is this?" | Định nghĩa bằng phạm vi công việc: "The title describes my mandate — I own data as a product: platform, modeling, BI, AI apps, và adoption bởi users. Senior-ity của tôi nằm ở 97% adoption và việc tôi là người duy nhất chịu trách nhiệm toàn chuỗi." Có sẵn bảng trách nhiệm nếu cần. |
| 7 | **Làm việc một mình** — chưa có kinh nghiệm team lớn, code review, on-call rota | "Have you worked in a larger data team?" | Thừa nhận rồi lật thành ý thức: "I've operated solo, so I built my own guardrails — CI/CD, tests, standards — because no one else would catch me. Điều tôi mong muốn ở team lớn là code review và on-call để học nhanh hơn." Cho thấy hiểu giá trị của quy trình team, không phải thợ một mình ngại quy trình. |
| 8 | **Độ sâu cloud** — AWS chỉ Lambda/SAM, GKE chỉ trong side project, không certification | "Your cloud experience looks limited" | Nếu JD cloud-heavy: lên kế hoạch lấy 1 cert nhanh (AWS SAA hoặc GCP PDE) trước phỏng vấn để trả lời "I'm currently preparing for...". Trong phòng phỏng vấn: nhấn mạnh nguyên lý vendor-agnostic (orchestration, modeling, lakehouse đều dịch chuyển được) + CryptoStream là bằng chứng GKE/Helm tay không. |
| 9 | **Metric AI (-60%) có thể bị hỏi cách đo** | "How exactly did you measure 60%?" | Chuẩn bị sẵn 1 câu: đo thời gian trung bình một buổi tư vấn trước/sau Salesaya trên cùng nhóm nhân viên sales (hoặc cách thực tế bạn đo) — nếu không đo được nghiêm ngặt, nói "ước lượng nội bộ dựa trên X" — trung thực vẫn thắng con số đẹp mờ hồ. |

**Ba nguyên tắc xử lý điểm yếu chung:**
1. **Không phủ nhận, không xin lỗi** — thừa nhận trong 1 câu, chuyển sang giá trị trong câu thứ hai.
2. **Mọi điểm yếu phải có "bằng chứng đang sửa"** — side project, cert đang học, quy trình đang áp dụng.
3. **Câu cuối rơi về thế mạnh** — kết mỗi câu trả lời về điểm yếu bằng điều bạn mang lại, không kết bằng lời xin lỗi.

---

## Phần C — Checklist trước buổi phỏng vấn

- [ ] Vẽ lại schematic kiến trúc Jemmia platform bằng tay trong 2 phút (whiteboard round gần như chắc chắn có)
- [ ] Chuẩn bị cách đo cho từng metric: 90% (chi phí Zapier vs self-host, tính thế nào), 60%, 3x, 97%, -12% WER
- [ ] Nếu có JD công ty cụ thể: thay câu hỏi ngược ở mỗi câu bằng câu chạm pain point từ JD
- [ ] Luyện 5/12 câu bằng tiếng Anh nếu công ty nước ngoài (1, 3, 4, 5, 8)
- [ ] Quyết định giữ/bỏ Kafka (và các item chưa defend được) trong skills.tex

---

## Phần D — Phụ lục Kafka: ôn cấp tốc

> Gắn với: hàng #5 Phần B (điểm yếu Kafka) và follow-up "Why no Kafka?" của Câu 1. Nguyên tắc framing: **hiểu chắc mô hình + biết chính xác khi nào KHÔNG dùng** — CDC một downstream thì nhúng Debezium; Kafka có chỗ khi có consumer thứ hai, burst, hoặc cần replay.

### D.1 — 1 hình nền

```
Producer ──ghi theo KEY──▶ Topic [P0|P1|P2] ──▶ Consumer Group (chia partition)
                             log append-only, giữ theo retention → REPLAY được
```

Kafka = **log phân tán**: A ghi 1 lần, B-C-D tự đọc từ chỗ mình dừng — tách producer khỏi consumer. Bốn vai trò trong một câu: *buffer* (hấp thụ burst), *bus* (một luồng nhiều người nghe), *storage* (giữ theo retention, đọc lại được), *ordering boundary* (thứ tự trong partition).

### D.2 — 5 câu thuốc đạn

| Câu hỏi | Câu trả lời 1 dòng |
|---|---|
| **Thứ tự?** | Chỉ trong partition. Cùng thực thể → ghi theo **key** (hash vào partition cố định) — chính là per-key ordering trong CDC |
| **Consumer group?** | Trong nhóm: chia partition (queue). Giữa nhóm: ai cũng nhận đủ (pub-sub). Offset mỗi nhóm riêng |
| **Mất message khi nào?** | Do cấu hình, không phải Kafka yếu: `acks=all` + RF≥3 + `min.insync.replicas≥2` + commit offset SAU xử lý |
| **Trùng message?** | At-least-once là mặc định → consumer phải **idempotent** (upsert theo PK) — replay vô hại |
| **Metric số 1?** | **Consumer lag** (offset cuối − offset đã commit) — tăng đơn điệu = chết/quá tải |

### D.3 — Lỗi hay gặp & fix

**Producer (đang ghi):**

| Triệu chứng | Nguyên nhân | Fix |
|---|---|---|
| Mất message lặng lẽ | `acks=0/1` — leader nhận nhưng replica chưa kịp sao, leader chết | `acks=all` + `min.insync.replicas≥2` + `replication.factor=3` |
| Trùng message | retry khi timeout (ghi thành công nhưng ack mất trên đường về) | `enable.idempotence=true` (PID + sequence number), hoặc consumer idempotent |
| Ghi chậm / timeout | batch nhỏ, gửi từng message | tăng `linger.ms` + `batch.size` — gom batch |
| NotEnoughReplicas | ISR tụt còn 1 (replica lag) | chữa sức khỏe broker/network, KHÔNG hạ `min.insync.replicas` để "qua loa" |

**Broker / cluster:**

| Triệu chứng | Nguyên nhân | Fix |
|---|---|---|
| Partition không có leader (unreadable) | leader chết, ISR rỗng, không tự bầu | chờ ISR bắt kịp; bầu bẩn (`unclean.leader.election`) = chấp nhận mất dữ liệu — cân nhắc kỹ |
| Đĩa đầy → cluster tê liệt | retention vô hạn hoặc message lớn bất thường | set retention theo thời gian/dung lượng, monitor disk %, alert trước 80% |
| Replica lag kéo dài | broker yếu / network chậm | tăng I/O, giảm tải partition khỏi broker nóng |

**Consumer (80% sự cố nằm đây):**

| Triệu chứng | Nguyên nhân | Fix |
|---|---|---|
| **Rebalance bão** — cả nhóm dừng liên tục | xử lý message quá lâu → hết session timeout → kick → rebalance → lặp | xử lý nhanh/async, tăng `max.poll.interval.ms`, cooperative rebalance |
| Consumer lag tăng dần | consumer chết hoặc quá tải | scale consumer **tới số partition** (nhiều hơn là idle), thêm partition |
| Đọc lại từ đầu sau restart | `auto.offset.reset=earliest` + group mới/chưa commit | đặt `latest` cho group mới, đảm bảo commit offset định kỳ |
| Skip message lặng lẽ | commit offset TRƯỚC khi xử lý xong, crash giữa chừng | commit **SAU** xử lý (at-least-once) + consumer idempotent |
| Duplicate sau deploy | đổi group id = group mới = đọc lại từ đầu | giữ group id ổn định qua deploy — quản lý như config quan trọng |

### D.4 — 2 câu chuyện debug (nếu bị hỏi kinh nghiệm)

- **Lag tăng đều mỗi ngày:** thêm consumer không ăn vì consumer đã bằng số partition — thêm nữa chỉ idle. Fix: tăng partition của topic trước (lưu ý: đổi partition count làm xáo trộn hash key — mất thứ tự trong lúc chuyển, phải chọn thời điểm), rồi mới scale consumer. *Bài học: partition count là trần trên của scale-out — quyết định sớm.*
- **Rebalance bão:** nhóm nhấp nháy stop/rebalance mỗi vài phút, throughput sập. Nguyên nhân: một batch message nặng khiến xử lý lâu hơn session timeout — coordinator tưởng consumer chết. Fix: tách batch nặng, tăng `max.poll.interval.ms`, xử lý bất đồng bộ. *Bài học: rebalance lặp luôn là dấu hiệu "xử lý chậm", không phải "cluster lỗi".*

### D.5 — Quy tắc vàng 5 dòng

1. **Mất = cấu hình**: `acks=all`, RF=3, `min.insync.replicas≥2`, commit sau xử lý
2. **Trùng = bình thường**: at-least-once + consumer idempotent (upsert theo PK)
3. **Lag là metric số 1** — tăng đơn điệu là sự cố đang cháy
4. **Rebalance lặp = xử lý chậm**, không phải cluster hỏng
5. **Partition count = trần scale consumer** — quyết định từ đầu, đổi sau đắt

**Câu đóng chủ đề:** *"Lỗi Kafka phần lớn không phải Kafka hỏng — là cấu hình delivery (acks/commit) và tốc độ consumer (lag/rebalance); mất thì do cấu hình, trùng thì chữa bằng idempotent. Và với dự án của tôi: CDC một downstream tôi nhúng Debezium thay vì Kafka — Kafka có chỗ khi có consumer thứ hai, burst, hoặc cần replay. Triết lý của tôi: at-least-once + consumer idempotent, không đốt tiền mua exactly-once."*
