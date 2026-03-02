# PROJECT_LOGIC_REVIEW — Rà soát bất hợp lý (logic / code / units / perf)

Mục tiêu tài liệu:
- Chỉ ra các điểm **bất hợp lý / rủi ro** trong dự án theo 4 nhóm: **logic**, **code bugs**, **units**, **performance/DB**.
- Phân loại mức độ **P0/P1/P2** theo tác động.
- Đưa **đề xuất sửa đổi cụ thể** (ngắn hạn + trung hạn) để bạn triển khai.

Phạm vi rà soát (những phần chạm trực tiếp kết quả WPA + API):
- Core compute: `analytics/computation/*`
- Persist models: `analytics/models.py`
- Raw SCADA schema: `acquisition/models.py`
- Turbines analysis APIs: `api_gateway/turbines_analysis/*`

Quy ước mức độ:
- **P0**: có thể làm **sai kết quả**, crash, hoặc trả payload sai nghiêm trọng (cần xử lý ngay).
- **P1**: rủi ro cao/ảnh hưởng trải nghiệm/khó debug/khó scale; chưa chắc sai ngay nhưng dễ “đổ”.
- **P2**: clean-code, refactor, tối ưu; làm để bền và dễ mở rộng.

---

## 1) Tóm tắt nhanh (high-signal)

### 1.1 P0 nổi bật
- **Unit mismatch pressure/power** giữa raw SCADA model và core compute (pressure “%” vs compute xử lý Pa; power MW vs nhiều threshold/constant assume kW).
- **Bug preprocessing HUMIDITY/PRESSURE** (pandas chained indexing + KNNImputer shape) → có thể **không lọc outlier** hoặc **crash** khi có cột met.
- **CapacityFactor** hiện tại **không phải capacity factor chuẩn** và công thức đang thiếu \(V^2\) so với power-in-wind → dễ gây hiểu nhầm KPI.

### 1.2 P1 nổi bật
- **Cache key collisions** (timeseries/working_period) khi truyền 1 trong 2 mốc thời gian → có thể trả nhầm dữ liệu cache.
- **Timestamp ms/s/ns** có nhiều nơi convert dựa trên ngưỡng `1e12`; ổn trong đa số trường hợp nhưng cần chuẩn hoá “single source of truth” để tránh edge-case.

---

## 2) P0 — Sai kết quả / crash / unit inconsistency

### P0-1) Unit mismatch: `FactoryHistorical.pressure` (%?) vs core compute expects **Pa**

**Vị trí**:
- Raw schema: `acquisition/models.py` (`FactoryHistorical.pressure` verbose_name “Air pressure (%)”)
- Core compute preprocess: `analytics/computation/normalize.py` lọc outlier `PRESSURE` trong [50000, 108500] (Pa)
- Air density formula: `analytics/computation/density.py` dùng `pressure / R_air` (đúng đơn vị Pa)

**Tác động**:
- Nếu dữ liệu thực sự là `%` (0..100) hoặc hPa, toàn bộ:
  - outlier removal sẽ **đánh dấu tất cả** là outlier hoặc giữ sai,
  - KNN impute sẽ “chế” ra pressure sai,
  - air density sai → normalize wind/power sai → power curve + KPI/AEP sai dây chuyền.

**Đề xuất sửa** (chọn 1 “đúng” và enforce):
- **Option A (khuyến nghị)**: canonical `PRESSURE` = **Pa** trong compute.
  - Ở ingestion/mapping (khi build DataFrame cho compute/timeseries), convert:
    - nếu raw là hPa → Pa: `Pa = hPa * 100`
    - nếu raw là kPa → Pa: `Pa = kPa * 1000`
    - nếu raw là `%` (không phải áp suất) → cần xác minh lại field, đổi tên hoặc bỏ khỏi density.
- **Option B**: nếu thật sự chỉ có “pressure %” (không vật lý) thì:
  - bỏ pressure ra khỏi density, luôn fallback `AIR_DENSITY` constant (hoặc tính density bằng T-only/h-only nếu có).

**Việc cần làm**:
- Audit dữ liệu thật (min/max/mean) của pressure trong DB.
- Chọn canonical unit và cập nhật docs + mapping.

---

### P0-2) Unit mismatch: `FactoryHistorical.active_power` (MW?) vs thresholds/constants assume kW

**Vị trí**:
- Raw schema: `acquisition/models.py` mô tả `active_power` “MW”.
- Constant estimation prefilter: `analytics/computation/constants_estimation.py` filter `ACTIVE_POWER <= 10000` (comment: 10 MW = 10000 kW).
- `analytics.Computation.p_rated` help_text: “kW”.
- KPI energy: `analytics/computation/indicators.py` tính năng lượng \(E=\sum P \cdot \Delta t\) theo unit của power.

**Tác động**:
- Nếu raw power thực tế là MW (0..6) mà code assume kW:
  - constant estimation sẽ cho `P_rated` ~ 5 (thay vì 5000),
  - các ngưỡng `0.85*P_rated`… thay đổi scale, classification/error filtering lệch,
  - energy/AEP scale sai (MWh vs kWh) → KPI & dashboard sai.

**Đề xuất sửa**:
- Chọn canonical unit cho `ACTIVE_POWER` trong compute (khuyến nghị **kW** để match thresholds hiện có).
- Enforce ở ingestion:
  - nếu raw MW → kW: `kW = MW * 1000`.
- Cập nhật verbose_name/metadata model cho đúng, hoặc giữ raw MW nhưng mapping sang compute kW.

---

### P0-3) Bug preprocessing met HUMIDITY/PRESSURE: chained indexing + KNNImputer shape

**Vị trí**: `analytics/computation/normalize.py`

**Triệu chứng** (trước khi vá):
- `data[mask]['HUMIDITY'] = np.nan` / `data[mask]['PRESSURE'] = np.nan` là chained indexing → có thể **không ghi** vào df gốc.
- `KNNImputer.fit_transform(data['HUMIDITY'])` truyền Series 1D → sklearn kỳ vọng 2D → dễ **crash** hoặc cho shape sai.

**Tác động**:
- Outlier không bị loại → density/normalize sai.
- Pipeline có thể fail khi có cột met.

**Trạng thái**:
- Đã vá:
  - dùng `data.loc[mask, col] = np.nan`
  - dùng `fit_transform(data[[col]]).ravel()`

---

### P0-4) “CapacityFactor” KPI hiện tại không đúng định nghĩa phổ biến + công thức bất thường

**Vị trí**: `analytics/computation/capacity_factor.py`

**Code hiện tại**:
\[
CF(bin)=\frac{\overline{P}_{bin}}{0.6125 \cdot A \cdot \overline{V}_{bin}}
\]
trong khi:
- capacity factor chuẩn thường: \(CF=\frac{E_{real}}{P_{rated}\cdot T}\)
- “power in wind” chuẩn: \(P_{wind} = 0.5\rho A V^3\) (không phải \(V\))

**Tác động**:
- Người dùng/FE hiểu nhầm (đặc biệt khi label “capacity factor”).
- Chỉ số có thể “đẹp” nhưng không có ý nghĩa đúng.

**Đề xuất sửa**:
- Nếu mục tiêu là capacity factor chuẩn:
  - implement `CapacityFactorOverall = RealEnergy / (P_rated * T_hours)`
  - persist vào `IndicatorData` hoặc bảng riêng.
- Nếu muốn giữ chỉ số hiện tại: rename + mô tả rõ (ví dụ `AerodynamicScalingIndex`).

---

## 3) P1 — Rủi ro cao / edge-cases / correctness-perf

### P1-1) Cache key collisions khi chỉ truyền 1 bound (start hoặc end)

**Vị trí**:
- `api_gateway/turbines_analysis/helpers/timeseries_helpers.py:get_cache_key`
- `api_gateway/turbines_analysis/helpers/working_period_helpers.py:get_cache_key`

**Issue**:
- Trước khi vá: `time_str = f"{start}_{end}" if start and end else "all"`
- `start!=None, end=None` dùng chung key với “không truyền start/end”.

**Tác động**:
- Trả nhầm payload cache (đúng turbine + sources + mode nhưng sai time-range).

**Trạng thái**:
- Đã vá: key luôn encode `none`/timestamp cho từng bound.

---

### P1-2) Timestamp normalization phân tán nhiều nơi (ms/s/ns) dựa trên heuristic

**Vị trí**:
- `api_gateway/turbines_analysis/helpers/_header.py` (convert timestamp)
- `api_gateway/turbines_analysis/helpers/computation_helper.py` (convert result start/end + point timestamps)
- `api_gateway/turbines_analysis/timeseries.py` + `timeseries_helpers.py`

**Tác động**:
- Heuristic `>1e12` nhìn chung ok (epoch ms ~ 1.7e12), nhưng:
  - data historical rất cũ hoặc test data có thể rơi vào “vùng mập mờ”
  - microseconds/nanoseconds cần phân biệt rõ.

**Đề xuất sửa**:
- Tạo 1 hàm chuẩn duy nhất (ví dụ `to_epoch_ms(value)`) và dùng lại ở tất cả modules.
- Enforce: mọi API input/output dùng **milliseconds**.

---

### P1-3) `verify_*` của NORMAL coverage có thể raise lỗi khó hiểu khi NORMAL rỗng

**Vị trí**: `analytics/computation/normalize.py` (`verify_min_hours`, `verify_bin_data_amount`, `verify_wind_coverage`)

**Tác động**:
- Nếu classification ra rất ít/không có NORMAL (data xấu), lỗi có thể mơ hồ hoặc crash do `.iloc[0]` trên empty.

**Trạng thái**:
- Đã bổ sung guard `if normals.empty: raise ValueError(...)` cho 2 hàm `verify_bin_data_amount` và `verify_wind_coverage`.
- Khuyến nghị: bổ sung tương tự cho `verify_min_hours`.

---

## 4) P2 — Clean-code / maintainability / performance tuning

### P2-1) `timestamp.rescale_resolution()` logic dư thừa + thông điệp lỗi chưa rõ

**Vị trí**: `analytics/computation/timestamp.py`

**Nhận xét**:
- `elif resolution < 60min: raise` và `else: raise` giống nhau.
- Nên nêu rõ: “chỉ hỗ trợ ≤10min (resample up) hoặc đúng 10min; coarser hơn thì reject”.

---

### P2-2) Performance: nhiều endpoint vẫn load raw SCADA + pandas transform

**Vị trí**: `api_gateway/turbines_analysis/*` (timeseries, cross_data_analysis, working_period, distribution…)

**Tác động**:
- Với farm lớn + time range dài, pandas thao tác lớn:
  - CPU spike, RAM spike
  - response time không ổn định

**Đề xuất sửa**:
- Ưu tiên “compute→persist→query-only” cho các view nặng:
  - materialize points cần cho scatter/cross analysis
  - materialize monthly aggregates
- Nếu vẫn phải đọc raw:
  - limit columns ngay từ DB query (chỉ select fields cần)
  - pagination / downsampling server-side (vd. LTTB, stratified sampling theo wind bins)
  - cache theo key chuẩn (đã có, nhưng cần key đúng + TTL hợp lý).

---

## 5) Đề xuất sửa đổi (roadmap thực thi)

### 5.1 Sửa ngay (P0 trong 1–2 ngày)
- **Chuẩn hoá unit**:
  - quyết định canonical unit cho `ACTIVE_POWER` (kW hay MW) và `PRESSURE` (Pa).
  - implement conversion tại ingestion/mapping cho compute + timeseries.
- **Fix bugs preprocessing met** (đã vá ở `normalize.py`).
- **Đổi tên hoặc thay công thức CapacityFactor** để tránh KPI sai nghĩa.

### 5.2 Nâng độ tin cậy (P1 trong 1 tuần)
- Centralize timestamp conversion (`to_epoch_ms`) và test bằng cases (s/ms/us/ns).
- Chuẩn hoá “normal coverage” guards để error message rõ.
- Hoàn thiện cache-key + cache invalidation theo version thuật toán (nếu keep-forever).

### 5.3 Tối ưu & bền vững (P2)
- Materialize cho cross-data + dashboard (đúng hướng “query-only APIs”).
- Add `algorithm_version`/`code_hash` vào `analytics.Computation`.
- DB partition/index tuning khi dữ liệu lớn.

