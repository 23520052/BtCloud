# KẾ HOẠCH ĐỒ ÁN AZURE STREAMING & POWER BI

**Đề tài:** Xây dựng hệ thống trực quan dữ liệu bán hàng dựa trên công nghệ xử lý luồng và phân tích thời gian thực trên Azure  
**Thời gian:** 07/11/2026 – 19/11/2026 (13 ngày)  
**Nhân sự:** 4 thành viên  
**Dataset:** [eCommerce Behavior Data from Multi-Category Store – Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv)  
**Stack:** Python → Azure Event Hubs → Azure Stream Analytics → Azure SQL Database → Power BI (DirectQuery)

## 1. Phạm vi và mục tiêu

- Dùng file `2019-Oct.csv` (khoảng 5,67 GB; khoảng 42,45 triệu event) với 4 loại event: `view`, `cart`, `remove_from_cart`, `purchase`.
- Python đọc CSV theo chunk, phát event lên Event Hubs để mô phỏng streaming. Có thể dùng khoảng 50% tập dữ liệu cho phát triển và phần còn lại để kiểm thử/demo; **không bắt buộc phát hết nửa còn lại trong buổi demo**.
- Stream Analytics tổng hợp theo cửa sổ 30 giây và 1 phút, ghi vào Azure SQL.
- Power BI có 4 trang report, navigation, slicer, tự làm mới, thử nghiệm RLS và mobile layout.
- Báo cáo được các phép đo tốc độ, độ trễ, so sánh và tối ưu dựa trên số đo thực tế; có Word, PPT, PBIX, GitHub và bằng chứng.

**Giới hạn dữ liệu:** `purchase` là *purchase event*, không chắc tương đương một đơn hàng do thiếu `order_id`. `purchase/view` là tỷ lệ giữa số event, không phải tỷ lệ chuyển đổi khách hàng duy nhất. Giữ cả `original_event_time` và `stream_event_time`; thêm `event_id` và `run_id` để kiểm tra, đối chiếu và phân biệt các lần chạy.

### Kiến trúc

```text
2019-Oct.csv (historical)
   ↓ Python prepare/replay, batch, rate limit, retry, checkpoint
Azure Event Hubs
   ↓
Azure Stream Analytics
   ├── 30s event metrics → dbo.event_metrics_30s ┐
   └── 1m brand sales   → dbo.brand_sales_1m  ├→ Azure SQL Database
                                                ↓
                                         Power BI DirectQuery
                                         4 report pages + refresh

Azure Blob Storage (tùy chọn): sample, logs, benchmark, lưu trữ hỗ trợ.
```

## 2. Phân vai và người chịu trách nhiệm

| Thành viên | Vai trò | Phạm vi chính | Nhánh Git |
|---|---|---|---|
| TV1 | Data Engineer | Dataset >4GB; schema event; Python Producer, replay, retry/checkpoint, throughput | `feature/data-producer` |
| TV2 | Streaming Engineer | Event Hubs, Stream Analytics, ETL/windowing, monitor/backlog | `feature/stream-analytics` |
| TV3 | Database & Cloud Engineer | Azure SQL, schema/index, Blob, read/write/region benchmark, chi phí | `feature/azure-sql` |
| TV4 | BI & QA Engineer | 4 trang Power BI, DAX, navigation, RLS, mobile, QA; đầu mối tổng hợp báo cáo | `feature/powerbi-dashboard` |

**Nguyên tắc:** phát triển song song theo *data contract* thống nhất ngày 08/11; dùng sample/mock trước khi tích hợp. Mỗi thành viên tự chịu trách nhiệm module, bằng chứng, tài liệu và phần trình bày của mình.

## 3. Mốc tổng thể

| Ngày / giờ | Milestone | Tiêu chí |
|---|---|---|
| **08/11** | Design / Data Contract Freeze | JSON schema, SQL schema, KPI, kiến trúc, quyền truy cập và handoff rõ ràng |
| **11/11 22:00** | Module Freeze | Producer, Event Hubs/ASA, SQL mock, Power BI 4 trang có thể test độc lập |
| **13/11 22:00** | End-to-End MVP | Chạy Python → Event Hubs → ASA → SQL → Power BI với 10.000 event |
| **16/11 22:00** | Feature Freeze | Benchmark, 4 trang, RLS, mobile, tối ưu và demo script |
| **18/11 22:00** | Final Review | Word, PPT, PBIX, mã nguồn, ảnh bằng chứng, diễn tập xong |
| **19/11 18:00** | Internal Submission | Bộ artifacts cuối sẵn sàng nộp; điều chỉnh nếu hạn thầy sớm hơn |

## 4. Sprint 1 — 07/11 đến 13/11: phát triển và tích hợp

### Lịch theo ngày

| Ngày | TV1 — Data | TV2 — Streaming | TV3 — Database | TV4 — Power BI |
|---|---|---|---|---|
| **07/11 (T7)** | Khảo sát CSV, thống kê dữ liệu | Thiết kế kiến trúc Azure | Thiết kế SQL schema | Chốt KPI, wireframe 4 trang |
| **08/11 (CN)** | Tạo sample JSON, data contract | Tạo Event Hubs và quyền Sender | Tạo Azure SQL Database | Chuẩn bị dashboard layout |
| **09/11 (T2)** | Viết Producer MVP | Tạo ASA Job, input | Tạo bảng, index, mock data | Kết nối SQL mock, làm trang 1 |
| **10/11 (T3)** | Batch, rate limiting, retry | ETL query và 2 output | SQL test, index | Làm trang 2–3 |
| **11/11 (T4)** | Checkpoint, chế độ dev/demo | Test ASA → SQL | Kiểm tra ASA output, SQL verification | Trang 4, navigation, slicer |
| **12/11 (T5)** | Gửi 10.000 event thật | Theo dõi Event Hubs → ASA | Kiểm tra dữ liệu ghi vào SQL | SQL thật, DirectQuery |
| **13/11 (T6)** | Sửa lỗi Producer | Sửa lỗi streaming | Sửa lỗi SQL, đối chiếu dữ liệu | Test realtime dashboard |

### TV1 — Deliverable Sprint 1, hạn **11/11 22:00**

- `data_profile.json`: dung lượng, tổng dòng, phân bố event, null, min/max timestamp.
- `data_contract.json`: schema JSON có `event_id`, `run_id`, `original_event_time`, `stream_event_time`, `event_type`, `product_id`, `brand`, `price`...
- `prepare_data.py`: sample và phân đoạn dev/demo có manifest, không cắt giữa dòng CSV.
- `producer.py`: gửi Event Hubs theo batch, giới hạn tốc độ.
- Retry, checkpoint sau batch được xác nhận; logging; `.env.example`, `requirements.txt`, `README.md`.
- **Nghiệm thu:** gửi thành công ≥10.000 event; không nạp toàn bộ CSV vào RAM; có log lỗi/thành công.

### TV2 — Deliverable Sprint 1, hạn **11/11 22:00**

- Namespace và Event Hub trên Azure, consumer group cho ASA, quyền Producer tối thiểu.
- Stream Analytics Job, Event Hubs input, Azure SQL output.
- `queries.sql`: ít nhất 2 query — `event_metrics_30s` (count/revenue theo event type), `brand_sales_1m` (purchase/revenue theo brand).
- `azure_setup.md`, cấu hình input/output, ảnh Portal, `stream_test.md`.
- **Nghiệm thu:** có Input Events và Output Events; ASA ghi thành công hai bảng SQL đúng schema.

### TV3 — Deliverable Sprint 1, hạn **11/11 22:00**

- Resource Group, Azure SQL Server/Database, quyền và firewall thích hợp.
- `schema.sql` và `indexes.sql` cho `dbo.event_metrics_30s`, `dbo.brand_sales_1m`.
- `seed_data.sql` cấp dữ liệu mock cho TV4 trước khi streaming sẵn sàng.
- `sql_validation.sql` đối chiếu thống kê; `database_setup.md`.
- Blob Storage container để lưu benchmark/sample (nếu dùng).
- **Nghiệm thu:** Azure SQL query được từ Power BI và có thể ghi output ASA.

### TV4 — Deliverable Sprint 1, hạn **11/11 22:00**

- `sales_dashboard.pbix` bản đầu từ dữ liệu SQL mock.
- 4 trang: **Sales Overview**, **Real-time Monitoring**, **Brand Performance**, **Customer Behavior**.
- `dax_measures.md`: event count, purchase events, purchase value, view events, purchase/view.
- Page Navigator, slicer và `dashboard_spec.md` (KPI/công thức/nguồn dữ liệu).
- **Nghiệm thu:** 4 trang mở được, navigation hoạt động, các KPI đọc đúng dữ liệu mock.

### Checkpoint nhóm **13/11 22:00**

1. TV1 phát 10.000 event gắn `run_id=integration-01`.
2. TV2 chứng minh Incoming Messages trong Event Hubs và output ASA theo window.
3. TV3 query Azure SQL thấy dữ liệu mới, đối chiếu aggregate với sample.
4. TV4 cho thấy Power BI Desktop cập nhật kết quả qua DirectQuery/Automatic Page Refresh.
5. Lưu ảnh, log, query, kết quả đối chiếu trong `evidence/`.

Nếu chưa đạt, ngày 14/11 ưu tiên sửa pipeline, chưa thêm feature mới.

## 5. Sprint 2 — 14/11 đến 19/11: benchmark, hoàn thiện, nộp

### Lịch theo ngày

| Ngày | TV1 — Data | TV2 — Streaming | TV3 — Database | TV4 — BI & QA |
|---|---|---|---|---|
| **14/11 (T7)** | Throughput benchmark 3–4 mức tải | Input/Output, Watermark Delay | SQL Read/Write benchmark | DirectQuery, Auto Page Refresh |
| **15/11 (CN)** | Tối ưu batch/retry/checkpoint | Tối ưu window, late events | Region latency, index | RLS, test 2 role |
| **16/11 (T2)** | Tập demo, resume test | Monitoring hoàn chỉnh | Tổng hợp benchmark, cost | Mobile layout, test 4 trang |
| **17/11 (T3)** | Viết dataset/Producer cho Word | Viết kiến trúc/ETL/lý thuyết | Viết storage/performance | Viết Power BI, tổng hợp Word/PPT |
| **18/11 (T4)** | Test Producer demo | Test ASA + Portal | Test SQL, xuất số liệu đo | Final PBIX, slide, QA, diễn tập |
| **19/11 (T5)** | Sẵn sàng chạy Producer | Sẵn sàng Azure Portal | Sẵn sàng SQL/benchmark | Sẵn sàng dashboard và nộp |

### TV1 — Deadline **16/11 22:00**

- `benchmark_producer.py`.
- `producer_metrics.csv`: events sent, success/error/retry, duration, events/s, MB/s; ≥3 mức tải.
- `benchmark_producer.md`: cách đo, môi trường, kết quả và so sánh.
- `demo_manifest.json` xác định phần dữ liệu demo, không trùng phần development.
- Producer có `--mode demo`, resume, tốc độ cấu hình được, hoạt động liên tục 5–10 phút.
- Screenshot log, throughput và README chạy demo.
- **Nghiệm thu:** benchmark tái hiện được, checkpoint hoạt động; số liệu không tự ước lượng.

### TV2 — Deadline **16/11 22:00**

- `stream_analytics.sql` bản cuối.
- `stream_metrics.csv`: input/output events, backlog, watermark delay, errors, SU utilization (nếu có).
- `stream_benchmark.md`: phương pháp đo, kết quả tại các mức tải, late/out-of-order handling.
- `azure_config.md`, ảnh Azure Monitor, truy vấn ASA và cấu hình.
- **Nghiệm thu:** streaming ổn định ở tải demo, backlog không tăng không kiểm soát, output khớp bảng SQL.

### TV3 — Deadline **16/11 22:00**

- `sql_benchmark.py`, `read_write_results.csv` cho kích thước/tải khác nhau.
- `region_latency_results.csv`, có mô tả endpoint, region/client, số lần đo, p50/p95 và hạn chế phép thử.
- `optimization.md`: index, aggregate, lựa chọn lưu trữ/tối ưu truy xuất.
- `cloud_cost_report.md`: tài nguyên, tier, region, thời gian chạy, chi phí/ước tính có căn cứ.
- `sql_validation.sql`, biểu đồ và ảnh benchmark.
- **Nghiệm thu:** kết quả đọc/ghi và region là số thực đo; so sánh cloud/local có nêu khác biệt cấu hình và giới hạn khoa học.

### TV4 — Deadline **16/11 22:00**

- `sales_dashboard.pbix` hoàn chỉnh 4 report, navigation và slicer.
- `dax_measures.md`, `dashboard_test_cases.md`, screenshots.
- DirectQuery và Automatic Page Refresh được kiểm chứng trên Power BI Desktop.
- RLS mẫu: `Manager` (tất cả brand), `Brand Analyst` (brand được phép), kiểm thử View as role; nếu có Service thì ghi rõ quyền/gán user thực.
- Mobile Layout và bằng chứng thử hiển thị.
- **Nghiệm thu:** chỉ số khớp SQL, 4 trang hoạt động, RLS và mobile được kiểm tra.

**Lưu ý:** cập nhật gần thời gian thực phụ thuộc cửa sổ ASA (30s/1m), ghi SQL và Power BI refresh. Nếu dùng Power BI Service shared capacity, giới hạn refresh khác Desktop; ưu tiên demo Desktop khi cần cập nhật nhanh.

## 6. Deadline tài liệu, review và nộp

| Deadline | Người | Deliverable |
|---|---|---|
| **17/11 18:00** | TV1 | `01_dataset_producer.md` + số liệu, log, ảnh |
| **17/11 18:00** | TV2 | `02_streaming_azure.md` + query, kiến trúc, monitor |
| **17/11 18:00** | TV3 | `03_database_performance.md` + benchmark/cost/ảnh |
| **17/11 18:00** | TV4 | `04_powerbi_qa.md` + RLS, mobile, screenshots |
| **17/11 22:00** | TV4 tổng hợp | Word và PowerPoint bản nháp |
| **18/11 12:00** | Cả nhóm | Review chéo lỗi font, số liệu, hình, kết luận, trích dẫn |
| **18/11 18:00** | TV4 tổng hợp | Word, PPT, PBIX final |
| **18/11 22:00** | Cả nhóm | Tổng duyệt demo, chốt GitHub/README/evidence |
| **19/11 18:00** | Cả nhóm | Bộ hồ sơ nộp hoàn chỉnh — **deadline nội bộ** |

Nếu hạn chính thức của giảng viên sớm hơn 19/11 18:00, phải dịch các mốc final tương ứng.

## 7. Handoff quan trọng giữa các thành viên

| Bàn giao | Bên nhận | Deliverable | Chậm nhất |
|---|---|---|---|
| TV1 | TV2 | `sample_events.json`, JSON schema | 08/11 12:00 |
| TV2 | TV1 | Event Hub name và quyền Sender | 08/11 22:00 |
| TV2 + TV3 | TV4 | Tên output, bảng SQL, schema và KPI contract | 08/11 22:00 |
| TV3 | TV4 | Azure SQL với mock data và quyền đọc | 09/11 22:00 |
| TV3 | TV2 | SQL tables và quyền ghi ASA | 10/11 18:00 |
| TV1 + TV2 | TV3 | Dữ liệu streaming thật | 12/11 12:00 |
| TV2 + TV3 | TV4 | Pipeline/SQL hoàn chỉnh để thử DirectQuery | 12/11 18:00 |

**Bảo mật:** không commit connection strings, mật khẩu hay secrets; chỉ đưa `.env.example` và tài liệu cách cấu hình.

## 8. Checklist rubric (10 điểm)

| Nhóm rubric | Điểm | Đầu việc / phụ trách |
|---|---:|---|
| Giới thiệu bài toán | 1,5 | Bài toán, dữ liệu, >4GB, so sánh cloud/traditional, mô hình dịch vụ — TV1/TV2/TV3/TV4 |
| Cơ sở lý thuyết | 1,5 | CSV/JSON/SQL/Blob, stream/window/ETL, mô tả dịch vụ Azure — TV1/TV2/TV3 |
| Mô hình dữ liệu | 2,0 | Tốc độ đọc/ghi, ETL tự động, độ trễ region, giải pháp tối ưu — TV1/TV2/TV3 |
| Hiện thực trực quan | 3,0 | 4 Power BI report + navigation, refresh, RLS, mobile — TV4/TV3 |
| Báo cáo và quản lý dự án | 2,0 | Word, PPT, demo, GitHub, phân chia vai trò và quản lý resource/cost — Cả nhóm |

**Các điểm dễ mất:**
- Chỉ trình bày lý thuyết tốc độ/độ trễ mà không có số thực đo.
- Nhầm *filter/slicer* với Row-Level Security.
- Chỉ tạo 4 visual trên **1 trang** thay vì 4 report pages có navigation.
- Chỉ có mock dashboard chứ không chứng minh stream → ASA → SQL → Power BI.
- Gọi `purchase event` là `order` dù dataset không có order ID.
- Không chụp ảnh Portal, monitor và quản lý tài nguyên.

## 9. Kịch bản demo

1. **TV1:** mở profile dataset >4GB, giải thích 4 event, bắt đầu Python Producer ở `--mode demo`; thấy event count / events per second.
2. **TV2:** vào Event Hubs Metrics cho thấy Incoming Messages tăng; mở ASA query/Monitor cho thấy Output Events và window ETL.
3. **TV3:** query Azure SQL theo `window_end` mới, đối chiếu output; trình bày benchmark SQL và region, tối ưu và chi phí.
4. **TV4:** Power BI Desktop DirectQuery cập nhật sau cửa sổ ASA; chuyển 4 trang, slicer, trình bày RLS và mobile layout.
5. **Cả nhóm:** tóm tắt benchmark, ưu/nhược điểm, giới hạn near-real-time và phương án cải tiến.

**Dự phòng:** có dữ liệu SQL đã nạp từ lần chạy trước, ảnh/video kết quả demo, screenshots quan trọng và file PBIX đã được kiểm thử. Không dùng kết quả dự phòng để khẳng định đang stream trực tiếp nếu pipeline đang lỗi.

## 10. Cấu trúc repository đề xuất

```text
azure-sales-streaming/
├── README.md
├── .gitignore
├── .env.example
├── requirements.txt
├── data/
│   ├── samples/
│   └── manifest.json
├── producer/                    # TV1
│   ├── prepare_data.py
│   ├── producer.py
│   └── benchmark_producer.py
├── stream-analytics/            # TV2
│   ├── queries.sql
│   ├── azure_setup.md
│   └── benchmark/
├── database/                    # TV3
│   ├── schema.sql
│   ├── indexes.sql
│   ├── seed_data.sql
│   └── benchmarks/
├── powerbi/                     # TV4
│   ├── sales_dashboard.pbix
│   ├── dax_measures.md
│   └── screenshots/
├── tests/
│   ├── test_data_integrity.py
│   └── test_pipeline.md
├── docs/
│   ├── data_contract.md
│   ├── architecture.md
│   ├── demo_script.md
│   └── project_report/
└── evidence/
    ├── event-hubs/
    ├── stream-analytics/
    ├── azure-sql/
    └── powerbi/
```

## 11. Quy định quản lý nhóm

- **Mỗi ngày 22:00:** cập nhật Issue/Project và đính kèm commit, PR, log, screenshot hoặc file kết quả; không có bằng chứng = chưa nghiệm thu.
- PR được ít nhất một thành viên khác review trước khi merge `main`.
- Từ **16/11:** không thêm feature ngoài rubric; tập trung fix lỗi và đo kiểm.
- Từ **18/11:** không thay schema/kiến trúc lớn; giữ cấu hình demo ổn định.
- **P0**: pipeline, dataset >4GB, Azure SQL, 4 trang dashboard, demo. **P1**: benchmark, region latency, RLS, mobile, tối ưu, báo cáo. **P2**: chỉ số/giao diện mở rộng.
- Cấu hình Azure budget alert; theo dõi Event Hubs, ASA, SQL và VM benchmark; dừng hoặc xóa resource đúng quy trình sau khi hoàn thành để tránh chi phí phát sinh.

## 12. Final acceptance — 19/11

- [ ] File gốc dataset >4GB được chứng minh bằng evidence.
- [ ] Python Producer đọc chunk và gửi event; có logging, retry, checkpoint, chế độ demo.
- [ ] Event Hubs nhận event và Stream Analytics xử lý ít nhất 2 loại aggregate.
- [ ] Azure SQL có dữ liệu mới và số liệu đối chiếu đúng.
- [ ] Power BI có 4 report pages, navigation, slicer, DirectQuery/refresh.
- [ ] RLS thử nghiệm với ít nhất 2 role; có Mobile Layout.
- [ ] Có benchmark Producer, SQL Read/Write, Azure streaming, region latency.
- [ ] Có ảnh chụp metrics và phương pháp đo để tái hiện.
- [ ] Word, PowerPoint, PBIX, scripts/config/docs đầy đủ.
- [ ] GitHub có đóng góp của cả 4 người, PR và lịch sử review.
- [ ] Đã chạy tổng duyệt end-to-end và có phương án dự phòng.
- [ ] Đã xem xét chi phí Azure và dọn tài nguyên sau sử dụng.

## 13. Tài liệu tham khảo

- [Dataset Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- [Azure Event Hubs: Send events with Python](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send)
- [Azure Stream Analytics window functions](https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-window-functions)
- [Azure Stream Analytics SQL Database output](https://learn.microsoft.com/en-us/azure/stream-analytics/sql-db-table)
- [Power BI Automatic Page Refresh](https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-automatic-page-refresh)
- [Power BI Row-Level Security](https://learn.microsoft.com/en-us/power-bi/enterprise/service-admin-rls)

---

**Lưu ý:** Các deadline trên là lịch nội bộ đề xuất cho nhóm; số liệu benchmark, chi phí và kết quả triển khai thực tế phải được bổ sung sau khi chạy thử, không được coi các giá trị minh họa là kết quả đo.
