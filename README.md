# ml-ops

## Note

- Branch: feature/data
- `./src/notebooks`: Nháp

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Use Case: Sinh Detailed Design cho `{DOMAIN_NAME}` trong `{SYSTEM_NAME}` từ Source Code

## Tên use case

Tự động phân tích source code PHP legacy của `{SYSTEM_NAME}` và sinh tài liệu Detailed Design cho `{DOMAIN_NAME}`, với `{DOMAIN_TABLE}` là bảng trung tâm.

## Tóm tắt

AI Agent đọc source code của `{SYSTEM_NAME}` và reverse-engineer các luồng nghiệp vụ chính xoay quanh `{DOMAIN_NAME}`.
Trong use case hiện tại:
- `{SYSTEM_NAME}` = `sns4`
- `{DOMAIN_NAME}` = reserve business flow
- `{DOMAIN_TABLE}` = `reserve`

Hệ thống là ứng dụng PHP v5 legacy, không sử dụng framework và dùng MySQL v8. Agent phải phân tích source đúng theo hiện trạng, không giả định các lớp hiện đại như controller, service, model, ORM hoặc routing nếu chúng không tồn tại trong source code.

Agent phải tập trung vào `{DOMAIN_TABLE}` và xác định các flow, bảng liên quan, rule, transaction, side effect và batch. Agent phải loại trừ toàn bộ folder `mig` và `config`. Các giá trị đến từ config được xem là tồn tại và hợp lệ, đồng thời phải ghi nhận là `Assumed from configuration`.

Output cuối cùng phải là một file Markdown Detailed Design Document theo các đầu mục IPA được liệt kê trong phần Output.

---
## Điều kiện tiên quyết

| # | Điều kiện | Mô tả |
|---|---|---|
| 1 | Source code input | Người dùng cung cấp source code folder của `{SYSTEM_NAME}`. |
| 2 | Runtime | Source code là PHP v5 thuần, không sử dụng framework. |
| 3 | Database | Database là MySQL v8. Các giá trị lấy từ config được xem là tồn tại và hợp lệ. |
| 4 | Domain table | Bảng trung tâm cần phân tích là `{DOMAIN_TABLE}`. Với use case hiện tại là `reserve`. |
| 5 | Excluded folders | Bỏ qua toàn bộ folder `mig` và `config`. Không đọc, phân tích hoặc trích dẫn source evidence từ các folder này. |
| 6 | Existing DD input | Nếu có DD cũ hoặc DD đã được generate, Agent có thể so sánh với source. |
| 7 | Execution | Agent chỉ đọc source code. Không chạy hệ thống, không connect database thật, không execute batch thật. |
| 8 | No unsupported assumption | Nếu thiếu evidence, đánh dấu điểm đó là `Need Confirmation`. |

## Output
Agent phải sinh một file Markdown chính:

```text
{SYSTEM_NAME}_DETAIL_DESIGN_DOCUMENT.md
```

File output phải theo các đầu mục Detailed Design kiểu dưới đây. Không tạo các tài liệu phân tích riêng như CRUD Matrix, Related Table Map hoặc Batch Impact Report độc lập. Các nội dung đó phải được đưa vào section tương ứng.

| # | Đầu mục Detailed Design kiểu IPA | Nội dung bắt buộc |
|---|---|---|
| 1 | Introduction | Purpose, scope, domain overview, definitions, acronyms, conventions, references và document overview. |
| 2 | Common Package and Mechanism | PHP include/function/class dùng chung, DB access, session/auth, permission, error handling, logging, performance và multilingual handling nếu có. |
| 3 | Diagrams | Mermaid sequence diagrams cho các flow chính của `{DOMAIN_NAME}` và ERD nếu đủ evidence. |
| 4 | Packages | Functional modules hoặc nhóm file/function được xác định từ source legacy. |
| 5 | Database | ERD, định nghĩa `{DOMAIN_TABLE}`, bảng liên quan, relationship, CRUD usage, index/constraint và stored procedure nếu có. |
| 6 | File Design | Danh sách source file screen/action/common/batch/JavaScript/template, ngoại trừ `mig` và `config`. |
| 7 | Code Design | Code organization, request/session handling, SQL strategy, validation, constants ngoài config, transaction và side effects. |
| 8 | Edge Case Definition | Abnormal values, boundary values và exception handling được trích xuất từ source condition và error handling. |
| 9 | Other Considerations | Limitations, assumptions, external dependencies, DD gaps, unresolved points và các mục `Need Confirmation`. |
| 10 | Appendix | Source Traceability Index, glossary, batch notes và supporting evidence references. |

## Kịch bản chính

### Bước 1 — Xác định phạm vi source và folder loại trừ

- Scan source folder của `{SYSTEM_NAME}`.
- Loại trừ mọi path chứa `/mig/` hoặc `/config/`.
- Không sử dụng file trong các folder bị loại trừ làm source evidence.
- Xác định các PHP screen/action files, common files, batch scripts, JavaScript files, templates và views còn lại nếu chúng ảnh hưởng đến request hoặc flow behavior.
- Ghi nhận source scope, excluded folders và warning nếu cấu trúc source không rõ.

### Bước 2 — Xác định kiến trúc PHP legacy và DB access

- Phân tích cách các file source ngoài config truy cập database.
- Xác định raw SQL strings, `mysql_*` / `mysqli_*` usage, DB wrappers, common query functions, include/require chains và transaction wrappers nếu có.
- Không trace connection values hoặc credentials bên trong `config`.
- Nếu code sử dụng constant hoặc value từ config, ghi `Assumed from configuration`.
- Ghi nhận DB access mechanism, query patterns và DB target nếu xác định được ngoài config, nếu không thì ghi `Need Confirmation`.

### Bước 3 — Tìm toàn bộ reference đến `{DOMAIN_TABLE}`

- Search tất cả source files không bị loại trừ để tìm `{DOMAIN_TABLE}`, related aliases, common key fields và SELECT/JOIN/INSERT/UPDATE/DELETE SQL.
- Với use case `reserve` hiện tại, bắt đầu từ các entry file đã biết như `reserve_form1.php`, các files trong folder `reserve` tương ứng nếu tồn tại.
- Với mỗi reference, ghi nhận file path, function/block, SQL operation, related tables, WHERE/JOIN conditions, input parameters, output behavior và source evidence.
- Đánh dấu các phần chưa rõ là `Need Confirmation`.

### Bước 4 — Phân loại business flows

- Gom các reference thành các business flow có ý nghĩa.
- Với `reserve`, kiểm tra search, list, detail, create, update, cancel, status change, date/time change, shop/staff/room handling, course/menu/treatment handling, vacant/availability, customer, payment, campaign, log/history và batch impact nếu có.
- Với domain khác, suy ra flow từ file names, screen/action names, request parameters, comments và SQL/table relationships.
- Bổ sung các flow được phát hiện thêm nếu evidence cho thấy chúng ảnh hưởng đến `{DOMAIN_TABLE}`.

### Bước 5 — Trace end-to-end từng flow

- Với từng flow, xác định entry point, form submit, AJAX endpoint, PHP action hoặc batch entry.
- Xác định request parameters, session values, hidden fields, query strings và POST data.
- Trace validation, normalization, permission/scope checks, business conditions, common function calls và SQL execution.
- Ghi nhận database impact lên `{DOMAIN_TABLE}` và related tables.
- Ghi nhận response, redirect, display message, updated data hoặc error handling.

### Bước 6 — Xác định related tables

- Trích xuất JOIN tables, subquery tables, tables updated trong cùng flow, master tables, log/history/status tables và batch/control tables.
- Phân loại từng table là main, detail/sub, master, transaction, log/history, derived/supporting hoặc batch/control data.
- Với mỗi table, mô tả relation với `{DOMAIN_TABLE}`, linking key, flow sử dụng, CRUD operations và mandatory/optional role.
- Đưa Related Table Map và table role description vào section Database hoặc Code Design.

### Bước 7 — Trích xuất business rules và validation

- Trích xuất required parameter rules, format rules, date/time validation, status transitions, duplicate/conflict checks, availability checks, permission/scope rules và cancel/update restrictions.
- Bổ sung customer/shop/menu/course/payment/campaign rules khi liên quan đến domain.
- Với mỗi rule, ghi nhận condition, source file/function, related table/field, pass/fail behavior và error/message nếu có.
- Đưa rule list vào Code Design và Edge Case Definition.

### Bước 8 — Phân tích transactions và side effects

- Search begin/commit/rollback, custom transaction wrappers và SQL failure handling.
- Với mỗi data-writing flow, xác định operations nằm trong và ngoài transaction, cùng rollback conditions.
- Xác định side effects như related table updates, log/history insertion, mail/notification, payment update, status update, batch trigger hoặc common function calls.
- Nếu thứ tự commit hoặc side effect chưa rõ, đánh dấu `Need Confirmation`.

### Bước 9 — Phân tích batch/script processing

- Tìm batch scripts trong top-level batch folders hoặc các batch locations không bị loại trừ.
- Search `{DOMAIN_TABLE}` và related tables trong batch scripts.
- Với mỗi batch liên quan, ghi nhận file path, purpose, trigger/schedule nếu có evidence ngoài config, input tables, output tables, main WHERE conditions và impact lên domain.
- Nếu schedule nằm trong config hoặc không tìm thấy, ghi `Trigger/Schedule: Assumed from configuration` hoặc `Trigger/Schedule: Need Confirmation`.

### Bước 10 — So sánh với Detailed Design đã có nếu được cung cấp

- Nếu DD cũ hoặc generated DD được cung cấp, so sánh source analysis với DD.
- Xác định missing flows, tables, relationships, rules, batches và edge cases.
- Xác định logic hoặc relationships bị mô tả sai.
- Mỗi gap phải có source evidence.
- Ghi DD Gap Analysis trong Other Considerations hoặc Appendix.

### Bước 11 — Sinh Detailed Design Document

Agent phải sinh `{SYSTEM_NAME}_DETAIL_DESIGN_DOCUMENT.md` theo cấu trúc này:
```text
1. Introduction
   1.1 Purpose
   1.2 Definitions, Acronyms and Abbreviations
   1.3 Standards and Conventions
   1.4 References
   1.5 Overview

2. Common Package and Mechanism
   2.1 Common Package
   2.2 Error, Exception Handling
   2.3 Log, Trace and Debug
   2.4 Performance Optimizing Mechanism
   2.5 Multilingual Processing

3. Diagrams

4. Packages

5. Database
   5.1 ERDs
   5.2 {DOMAIN_TABLE} Table
   5.3 Related Tables
   5.4 CRUD Matrix
   5.5 Store Procedure

6. File Design
   6.1 File List
   6.2 Configuration Assumptions

7. Code Design

8. Edge Case Definition
   8.1 Abnormal Value Definition
   8.2 Boundary Value Definition
   8.3 Exception Handling Definition

9. Other Considerations

10. Appendix
```

Notes:
- Điều chỉnh nội dung cho PHP v5 legacy.
- Nếu hệ thống là procedural PHP, mô tả theo file/function/block-based design thay vì ép theo class-based design.
- Không liệt kê file thuộc `mig` hoặc `config` trong File Design source list.
- Các giá trị liên quan config phải được ghi như assumption, không phải traced evidence.

### Bước 12 — Self Validation

Trước khi hoàn tất, kiểm tra:

- Không có source trace nào trỏ đến `/mig/` hoặc `/config/`.
- Không dùng config file làm source evidence.
- Config values được ghi là `Assumed from configuration`.
- Main flows có source trace.
- CRUD operations trên `{DOMAIN_TABLE}` đã được phân loại.
- Related tables có business role descriptions.
- Batch processing đã được kiểm tra.
- Edge cases là source-based.
- Missing evidence được đánh dấu là `Need Confirmation`.
- Output cuối cùng là Detailed Design theo chuẩn quy định trong Mục Output

Validation status:

- `PASSED`, hoặc
- `ACTION REQUIRED`

## Kịch bản phụ

### A — Không tìm thấy source folder

- Dừng xử lý.
- Thông báo source folder không tồn tại hoặc không đọc được.
- Không tạo DD dựa trên giả định.

### B — Không tìm thấy `{DOMAIN_TABLE}`

- Search aliases, id/code fields, domain synonyms và Japanese/English business terms nếu áp dụng.
- Nếu vẫn không tìm thấy, dừng phân tích domain.
- Output `No reference found for {DOMAIN_TABLE}`.
- Không tạo flow giả định.

### C — Dynamic SQL quá phức tạp

- Ghi lại partial SQL và related file/function.
- Đánh dấu phần chưa rõ là `Need Confirmation`.
- Không giả định JOIN/WHERE conditions nếu không có evidence.

### D — Không xác định được business flow

- Phân loại tạm theo CRUD, screen/action hoặc batch.
- Đánh dấu business meaning là `Need Confirmation`.
- Đưa vào ambiguity list.

### E — Có nhiều entry point cho cùng operation

- Không gộp một cách tùy tiện.
- Phân loại riêng theo screen/action, request parameter, user role/scope nếu xác định được và business context.
- Chỉ consolidate khi có đủ evidence.

### F — Không xác định được database target

- Nếu phụ thuộc config, ghi `Assumed from configuration`.
- Nếu evidence vẫn không đủ, ghi `Database target: Need Confirmation`.
- Vẫn ghi nhận SQL operation và source file.

### G — Batch xử lý domain nhưng schedule không rõ

- Vẫn mô tả batch logic.
- Nếu schedule nằm trong config, ghi `Assumed from configuration`.
- Nếu không có evidence, ghi `Need Confirmation`.

### H — Existing DD mâu thuẫn với source

- Ưu tiên source code làm primary evidence.
- Ghi mismatch vào DD Gap Analysis.
- Không sửa DD cũ nếu không được yêu cầu rõ ràng.

### I — Phát hiện flow ngoài danh sách ban đầu

- Không bỏ qua flow.
- Thêm vào Business Flow Candidate List.
- Giải thích vì sao flow liên quan đến `{DOMAIN_TABLE}`.
- Bổ sung source evidence.

## Ràng buộc phi chức năng

| # | Loại | Ràng buộc |
|---|---|---|
| 1 | Source-based only | Kết luận phải dựa trên source code hoặc DD/schema được cung cấp. Không suy luận nghiệp vụ nếu không có evidence. |
| 2 | System scope | Chỉ phân tích `{SYSTEM_NAME}`. Hệ thống khác chỉ được ghi như external dependency nếu source reference trực tiếp. |
| 3 | Excluded folders | Không đọc, phân tích hoặc trích dẫn source evidence từ `mig` hoặc `config`. |
| 4 | Config assumption | Giá trị từ config được xem là tồn tại và hợp lệ. Không trace vào config. |
| 5 | Legacy PHP understanding | Hiểu PHP v5 procedural/custom structure và không giả định framework/ORM/service-layer patterns. |
| 6 | Domain-centered analysis | Ưu tiên `{DOMAIN_TABLE}`. Flow/table khác chỉ đưa vào nếu liên quan trực tiếp đến domain chính. |
| 7 | Business detail level | Mô tả ý nghĩa nghiệp vụ của flow, table và rule. Không chỉ liệt kê SQL. |
| 8 | Traceability | Kết luận quan trọng phải có file path, function/block, line range hoặc SQL snippet. |
| 9 | No destructive execution | Không execute write query, modify DB hoặc chạy batch thật. |
| 10 | Ambiguity handling | Nếu thiếu evidence, ghi `Need Confirmation` và giải thích cần xác nhận gì. |
| 11 | Batch coverage | Batch/scripts liên quan domain phải được đưa vào DD. |
| 12 | DD format compliance | Output cuối cùng là Detailed Design theo chuẩn quy định trong Mục Output |
| 13 | Adaptable domain | Use case này phải tái sử dụng được cho domain khác bằng cách thay `{DOMAIN_NAME}` và `{DOMAIN_TABLE}`. |

## Checklist review nhanh

| # | Mục kiểm tra | Kỳ vọng |
|---|---|---|
| 1 | Scope | Output chỉ tập trung vào `{SYSTEM_NAME}`. |
| 2 | Excluded folders | Không có source trace nào trỏ đến `mig` hoặc `config`. |
| 3 | Config values | Config-derived values được ghi là `Assumed from configuration`. |
| 4 | DD headings | Output dùng IPA-style headings từ Introduction đến Appendix. |
| 5 | CRUD Matrix | CRUD operations trên `{DOMAIN_TABLE}` được phân loại theo file/function/block. |
| 6 | Business flows | Main flows được mô tả khi tồn tại trong source. |
| 7 | Additional flows | Flow phát hiện thêm được bổ sung kèm evidence. |
| 8 | Related tables | Related tables có business role descriptions. |
| 9 | Business rules | Validation/rules được trích xuất từ source. |
| 10 | Transaction/side effects | Commit/rollback/log/status/payment/mail/batch impacts được mô tả nếu tìm thấy. |
| 11 | Batch impact | Batch liên quan domain được mô tả với input/output/table impact. |
| 12 | Edge cases | Abnormal/boundary/exception cases dựa trên source. |
| 13 | Source trace | Kết luận quan trọng có evidence. |
| 14 | Ambiguity | Điểm chưa rõ được đánh dấu `Need Confirmation`. |
| 15 | DD output | Output cuối cùng là `{SYSTEM_NAME}_DETAIL_DESIGN_DOCUMENT.md`. |
