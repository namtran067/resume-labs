# Plan: Thêm kinh nghiệm Long Châu vào CV `cv/data-engineer-T10-2026`

## Ngữ cảnh
- CV `cv/data-engineer-T10-2026/` hiện là bản sao y hệt `cv/data-engineer-jemmia` (Product Data Engineer, 2 trang).
- Cần thêm job mới lên đầu Experience: **Data Engineer @ Nhathuoc Long Châu** (Aug 2026 – Present).
- Jemmia kết thúc **Jul 2026** (xác nhận từ user; hiện đang ghi "Sep 2023 -- Present").
- Số thật của Long Châu: **2,000+ bảng**, **400+ source databases**, **120+ Kafka topics**, bảng lớn nhất **100M+ rows/ngày**. Stack: Databricks, Azure Data Factory.
- Chính sách cắt: **tối thiểu** — giữ nguyên toàn bộ experience cũ, chỉ lược projects; chấp nhận CV dài hơn 2 trang.

## Thay đổi

### 1. Tạo file mới `cv/data-engineer-T10-2026/experience/long-chau.tex`
4 bullet, format `[Tag]` giống jemmia, chỉ dùng số thật (không cần bịa thêm — số đã đủ mạnh):

```latex
\headingBf{Data Engineer | Nhathuoc Long Châu}{Aug 2026 -- Present}
\vspace{1pt}
\begin{resume_list}
  \item \textbf{[Ingestion at Scale]} Operate and maintain ingestion of \textbf{2{,}000+ tables} from \textbf{400+ source databases} into the bronze (raw) layer -- largest tables \textbf{100M+ rows/day} -- the reliable raw supply every downstream team builds on.
  \item \textbf{[Silver Layer]} Build base transformations from bronze to silver, delivering cleansed, conformed datasets consumed by downstream analytics and data teams.
  \item \textbf{[Platform Migration]} Key contributor migrating pipelines from Azure Data Factory + Databricks to a \textbf{full-Databricks platform} -- removing the ADF hop to cut latency, cost and operational complexity.
  \item \textbf{[Streaming \& Batch Standards]} Stand up streaming (\textbf{120+ Kafka topics}) and batch pipelines on Databricks and \textbf{define the team's standards} for both patterns -- consistent development, monitoring and on-call.
\end{resume_list}
```

### 2. Sửa `resume.tex`
- Thêm vào đầu section Experience (trước jemmia):
  ```latex
  \input{experience/long-chau.tex}

  \needspace{4\baselineskip}
  \vspace{2pt}
  ```
- Bỏ 3 project: `\input{projects/cryptostream.tex}`, `\input{projects/side-projects.tex}`, `\input{projects/sign2text.tex}` (kèm `\vspace{10pt}` đi kèm). Giữ `minilakehouse` + `fb-feed-crawler`.

### 3. Sửa `experience/jemmia-diamond.tex`
- Line 7: `Sep 2023 -- Present` → `Sep 2023 -- Jul 2026`.

### 4. Sửa `skills.tex`
- Dòng "Data Engineering" đưa stack mới lên đầu: `Databricks (Delta Lake, medallion), Azure Data Factory, Apache Kafka, dbt, Dagster, Airflow, dlt, Airbyte, n8n, ETL/ELT`.
- Dòng "Cloud, DevOps": thêm `Azure` vào danh sách hiện có.
- Lưu ý: Kafka giờ được giữ chính đáng (có production usage thật ở Long Châu) — khác với khuyến nghị cũ "bỏ Kafka khỏi skills" trong `docs/interview-prep/` (khuyến nghị đó giờ đã lỗi thời với CV này).

### 5. Sửa `summary.tex` (khuyến nghị — nhẹ tay)
Thêm 1 mệnh đề về scale hiện tại để hook đầu CV khớp job mới, ví dụ cuối đoạn:
`... now operating 2{,}000+ table ingestion (400+ sources, 100M+ rows/day) on Databricks at Long Châu.`

## Validation
1. Build: `tectonic cv/data-engineer-T10-2026/resume.tex` (từ repo root).
2. Page count qua `osascript` JXA `PDFDocument.pageCount` (mdls hay bị stale).
3. Trích text từng trang (JXA `pageAtIndex(i).string`) kiểm tra: Long Châu nằm đầu trang 1 section Experience; không có heading mồ côi cuối trang (nếu có → thêm `\needspace` trước block tương ứng).
4. Chấp nhận >2 trang (user đã chọn "cắt tối thiểu"); nếu thành 3 trang mà trang 3 vơi (<1/3) → báo user, gợi ý cắt tiếp AISIA hoặc 1-2 bullet Jemmia.

## Out of scope
- Không sửa `cv/data-engineer-jemmia/` và `cv/product-operation-lead-supra/`.
- Không update docs interview-prep.
