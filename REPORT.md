# Báo cáo LAB 17 — Data Pipeline Engineering

## 0 · Kết quả kiểm chứng make verify

PS E:\Day17-Track2-2A202601324-TranHoangMaiAnh> python tools/verify.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 58.4s
run 2/3 … 73.1s
run 3/3 … 71.9s

BẢNG ỔN ĐỊNH SỐ HÀNG KỲ VỌNG GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set ✓ ok 12,480 12,480 ✓
gold_feature_daily ✓ ok 9,100 9,100 ✓
gold_doc_chunks ✓ ok 31,200 31,200 ✓
quarantine_tickets ✓ ok 312 312 ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set 8dd7c98653 8dd7c98653 8dd7c98653 ✓
gold_feature_daily 3db448685c 3db448685c 3db448685c ✓
gold_doc_chunks 92d8e50131 92d8e50131 92d8e50131 ✓
quarantine_tickets ebb89036fb ebb89036fb ebb89036fb ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL ✓ sạch
quarantine_tickets đúng số bản ghi lỗi ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket ✓ không lặp
dashboard rows scanned ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
số file parquet ✓ 5,000 → 14
kết quả truy vấn không đổi ✓
DAG: catchup / max_active_runs ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓ 1 · gold_training_set idempotent & đúng số hàng
✓ 2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓ 3 · contract + quarantine + dbt test
✓ 4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt

Ba lượt pipeline cho cùng checksum ở cả bốn bảng:

| Bảng                 | Số hàng | Checksum ba lượt |
| -------------------- | ------: | ---------------- |
| `gold_training_set`  |  12.480 | `8dd7c98653`     |
| `gold_feature_daily` |   9.100 | `3db448685c`     |
| `gold_doc_chunks`    |  31.200 | `92d8e50131`     |
| `quarantine_tickets` |     312 | `ebb89036fb`     |

`dbt test` pass 11/11. `silver_tickets.priority` sạch (không `NULL`, chỉ 1..4), `gold_training_set` không có `ticket_id` lặp và DAG đạt `catchup=False` / `max_active_runs=1`.

## 1 · `gold_training_set` tăng hàng khi chạy lại

|                |                                                                                                                                                                                                                                                                                                   |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Triệu chứng    | Bảng incremental tăng hàng và lặp ticket sau retry/rerun.                                                                                                                                                                                                                                         |
| Nguyên nhân    | Grain là entity, một hàng mỗi `ticket_id`, nhưng incremental không có `unique_key` nên dbt append. CDC có update: một ticket xuất hiện ở ngày tạo và ngày cập nhật; khi được xử lý lại, bản ghi mới bị thêm thay vì thay thế. Vì thế retry scheduler biến write không idempotent thành duplicate. |
| Cách khắc phục | `gold_training_set.sql`: thêm `unique_key='ticket_id'` và strategy `merge` để mỗi lần chạy lại cập nhật hàng có cùng khóa thay vì thêm hàng mới. DAG đặt `catchup=False`, `max_active_runs=1` để giảm số lần rerun/chạy chồng; dù vậy, chỉ `merge` trong model mới bảo đảm rerun không tạo trùng. |
| Bằng chứng     | 12480 hàng, không có ticket lặp, checksum ổn định qua ba lượt.                                                                                                                                                                                                                                    |

## 2 · `gold_feature_daily` thiếu hàng lịch sử

|                    |                                                                                                                                                                                    |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Triệu chứng        | Thiếu các cặp `(event_date, customer_id)` ở ngày cũ; bảng có 8645 thay vì 9100 hàng.                                                                                               |
| P99 độ trễ đo được | 27258 ngày, có 5,0509% event đến muộn hơn một ngày.                                                                                                                                |
| Lookback đã chọn   | 3 ngày, làm tròn lên từ P99.                                                                                                                                                       |
| Nguyên nhân        | Bộ lọc chỉ lấy `event_date > max(event_date)` của target. Event xảy ra ngày cũ nhưng đến kho sau khi target đã tiến qua ngày đó không bao giờ thỏa điều kiện, nên bị bỏ vĩnh viễn. |
| Cách khắc phục     | Recompute `event_date >= max(event_date) - interval 3 day` và merge theo khóa ghép `['event_date', 'customer_id']` để tái tính không tạo duplicate.                                |
| Bằng chứng         | 9100 hàng và checksum ổn định qua ba lượt.                                                                                                                                         |

P99 phù hợp làm căn cứ vận hành vì bao phủ phần lớn dữ liệu đến muộn với chi phí tính lại cố định mỗi lượt. Chọn `max` sẽ làm lookback lớn hơn để xử lý ngoại lệ hiếm, đồng nghĩa mọi lượt sau đều phải quét/tính lại thêm ngày; P99 cần được theo dõi để điều chỉnh khi phân phối độ trễ thay đổi.

## 3 · `priority` đổi biểu diễn giữa chu kỳ

|                  |                                                                                                                                                                                                                                                                        |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Triệu chứng      | Nhãn chuỗi bị ép thành `NULL`, trong khi các số ngoài 1..4 vẫn qua được; downstream nhận 6606 priority sai.                                                                                                                                                            |
| Nguyên nhân      | `try_cast` chỉ kiểm tra khả năng ép kiểu, không kiểm tra domain hay schema evolution. Nó không hiểu nhãn hợp lệ (`urgent/high/medium/low`) và chấp nhận `0`, `5`, `-1` dù vi phạm contract. Nếu lọc sau khi xếp hạng CDC, cập nhật lỗi mới nhất còn làm mất cả ticket. |
| Ba nhóm và xử lý | Số `1..4`: giữ nguyên; nhãn hợp lệ: map `urgent/high/medium/low → 1/2/3/4`; rỗng, `NULL`, `P1`, `unknown`, và số ngoài miền: quarantine.                                                                                                                               |
| Cách khắc phục   | Macro `CASE` dùng chung cho Silver/quarantine; lọc record không chuẩn hóa được trước `row_number`; bật contract và thêm `not_null`, `accepted_values`.                                                                                                                 |
| Bằng chứng       | `quarantine_tickets=312`, `silver_tickets=12.480` ticket, priority sạch, `dbt test` 11/11 pass.                                                                                                                                                                        |

Nên kiểm soát contract ở Silver: tầng Bronze giữ bản ghi gốc để truy vết, còn Silver là lớp chuẩn hóa cho downstream. Pipeline không nên dừng vì vài trăm bản ghi lỗi sẽ chặn toàn bộ dữ liệu hợp lệ, vùng quarantine sẽ giữ đầy đủ record lỗi để xử lý sau, không che giấu chất lượng dữ liệu.

## 4 · Mở rộng A — Dashboard chậm vì small-file problem

|                |                                                                                                                                                                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Triệu chứng    | Dashboard quét 5.000.000 rows scanned từ 5.000 file dù dữ liệu thực chỉ có 130.683 hàng.                                                                                                                                                                                       |
| Nguyên nhân    | Dataset không partition nên phải mở toàn bộ file trước khi biết ngày/khách hàng có phù hợp. Predicate `strftime(event_time, ...)` bọc cột trong hàm, khiến engine không thể dùng partition pruning hay statistics. Các file vài chục hàng làm chi phí đọc bị làm tròn theo lô. |
| Cách khắc phục | Compact thành Parquet partition theo `event_date`, sort `event_date, customer_name`, row group 2048 hàng. Query đọc dataset mới với `hive_partitioning = true` và lọc sargable `event_date = DATE '2026-08-09'`.                                                               |
| Bằng chứng     | 5000000 giảm còn 9324 rows scanned, 5000 giảm còn 14 file, 130683 hàng không mất, result hash giữ nguyên `4379e4c5d9f3`.                                                                                                                                                       |

## 5 · Mở rộng B — Consumer crash giữa batch

|                |                                                                                                                                                                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Triệu chứng    | Commit offset trước write tạo at-most-once: crash giữa hai thao tác làm offset đi trước dữ liệu và batch bị mất khi restart.                                                                                                                                                   |
| Nguyên nhân    | Offset transport và ghi DuckDB là hai side effect riêng, không có transaction chung. Đổi sang write trước/commit sau thì crash có thể replay batch, và `INSERT` thuần sẽ gây duplicate.                                                                                        |
| Cách khắc phục | Ghi batch trước điểm crash, chỉ commit offset sau write (at-least-once), thêm primary key `event_id` và upsert `ON CONFLICT DO UPDATE` để write idempotent, update giữ payload mới nhất nếu message replay có nội dung thay đổi, khác với `DO NOTHING` vốn bỏ qua thay đổi đó. |
| Bằng chứng     | Crash ở lô 7 với offset 3000, restart ghi nốt 17.000 message; cuối cùng 20000 hàng / 20000 event ID khác nhau, không mất, không trùng và `C == A`.                                                                                                                             |

Kết quả chạy lệnh: python tools\crash_test.py --strict

topic: 20,000 message · batch 500 · giết ở lô 7

A. chạy một mạch, không sự cố
[consumer] đã ghi 20,000 message
-> 20,000 hàng / 20,000 event_id khác nhau

B. chạy và bị giết ở lô 7
[consumer] 💥 tiến trình bị giết ở lô 7
-> tiến trình thoát với mã 137
-> offset đã commit: 3,000

C. khởi động lại, chạy nốt
[consumer] đã ghi 17,000 message
-> 20,000 hàng / 20,000 event_id khác nhau

---

không mất bản ghi ✓
không trùng bản ghi ✓
C == A ✓

---

BÀI MỞ RỘNG B: ĐẠT ✓
