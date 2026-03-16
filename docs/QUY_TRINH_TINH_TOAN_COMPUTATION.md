# Quy trình tính toán & công thức chi tiết (module `analytics/computation`)

Tài liệu này mô tả **pipeline tính toán**, **công thức**, **nguồn gốc công thức** và **định nghĩa trạng thái phân loại** đang được implement trong package `analytics/computation/` của SmartWPA. Phần cuối tài liệu đối chiếu với tiêu chuẩn **IEC 61400-12-1:2022** (Power performance measurements of electricity producing wind turbines).

- **Entry-point**: `analytics/computation/smartWPA.py` (`get_wpa`, `process`)
- **Dữ liệu đầu vào**: SCADA time-series (mặc định kỳ vọng 10 phút) với các cột chuẩn hóa theo `analytics/computation/normalize.py`.

---

## 0) Data-flow end-to-end (API → compute → persist → API đọc DB)

SmartWPA được tổ chức theo 2 lớp:

- **API/Persist layer**: `api_gateway/turbines_analysis/computation.py` + `api_gateway/turbines_analysis/helpers/computation_helper.py`
- **Core compute layer**: `analytics/computation/*` (WPA pipeline)

Luồng chuẩn khi FE/Client gọi computation:

```mermaid
flowchart TD
  A[Client/FE] --> B[POST /api/turbines/{id}/computation/]
  B --> C[load_turbine_data(db->file fallback)]
  C --> D[derive_turbine_constants_from_scada]
  D --> E[get_wpa(df, constants)]
  E --> F[save_computation_results]
  F --> G1[Computation(type=classification) + ClassificationPoint/Summary + FailureEvent]
  F --> G2[Computation(type=power_curve) + PowerCurveAnalysis/PowerCurveData]
  F --> G3[Computation(type=indicators) + IndicatorData + DailyProduction + CapacityFactorData]
  F --> G4[Computation(type=weibull) + WeibullData]
  F --> G5[(optional) Computation(type=yaw_error) + YawErrorData/Statistics]
  A --> H[GET analysis APIs đọc DB]
```

Tại sao phải persist:
- API “analysis pages” chạy nhanh và ổn định
- Tránh recompute từ raw SCADA mỗi lần mở UI

---

## 1) Tổng quan pipeline (theo code)

Luồng tính toán hiện tại có 2 lớp:

- **Lớp API/Persist (api_gateway)**: load dữ liệu → **ước lượng turbine constants từ SCADA** → gọi `get_wpa()` → lưu DB (overwrite đúng) → FE gọi các API đọc DB.
- **Lớp Core compute (analytics/computation)**: chạy pipeline WPA trong `get_wpa(data, constants)` như sơ đồ dưới.

Luồng chính trong `get_wpa(data, constants)` (core compute):

```mermaid
flowchart TD
  A[Raw SCADA DataFrame] --> B[preprocess: timestamp + met cleaning]
  B --> C[classify: gán status vận hành]
  C --> D[lọc NORMAL]
  D --> E[air_density]
  E --> F[normalize_data theo rho]
  F --> G[binning theo WIND_SPEED]
  G --> H[get_all_power_curves]
  D --> I[weibull fit (wind)]
  H --> J[rayleighs_aep + AepWeibullTurbine]
  C --> K[indicators: energy KPIs + yaw + reliability]
  G --> L[capacity_factor]
  C --> M[classification_to_obj]
  A --> N[start_time/end_time]
```

Các module/hàm tương ứng:
- **Preprocess**: `normalize.preprocess()` → gọi `timestamp.timestamp_prepare()`
- **Classification**: `classifier.classify()`
- **Air density**: `density.air_density()`
- **Normalize theo rho**: `normalize.normalize_data()`
- **Binning**: `bins.binning()`
- **Power curve**: `curve_est.get_all_power_curves()`
- **Weibull**: `weibull.weibull()`
- **AEP (Rayleigh/Weibull)**: `rayleighs.rayleighs_aep()`
- **Indicators (Energy + Reliability + Yaw)**: `indicators.indicators()`
- **Capacity factor**: `capacity_factor.capacity_factor()`

---

## 2) Chuẩn dữ liệu đầu vào & giả định đơn vị

### 2.1 Cột bắt buộc
Theo `normalize.required_column_names`:
- `TIMESTAMP`
- `WIND_SPEED`
- `ACTIVE_POWER`

Các cột tùy chọn (nếu có thì dùng):
- `DIRECTION_NACELLE`, `DIRECTION_WIND` (tính yaw error)
- `HUMIDITY`, `PRESSURE`, `TEMPERATURE` (tính air density)

### 2.2 Timestamp & sampling
Theo `timestamp.timestamp_prepare()`:
- Xóa trùng `TIMESTAMP`, giữ bản ghi đầu (`drop_duplicates`)
- `set_index('TIMESTAMP')`
- Xác định **resolution** = mode của sai phân timestamp: **Δt** = mode(Δ TIMESTAMP).
- Nếu dữ liệu **finer** hơn 10 phút: resample về `10min` bằng **mean**
- Nếu dữ liệu **coarser** hơn 10 phút (30/60 phút…): raise error (code hiện tại báo “Time resolution is too low”)
- Fill missing timestamps: `reindex` trên dải `pd.date_range(..., freq='10min')` → tạo NaN cho các điểm thiếu

### 2.3 Giả định đơn vị
Tài liệu này mô tả theo implementation:
- `WIND_SPEED`: m/s
- `ACTIVE_POWER`: đơn vị công suất SCADA (có thể kW hoặc MW tuỳ OEM). Năng lượng được tính theo **E** ≈ Σ **P**·**Δt** (đổi **Δt** sang giờ).
- `TEMPERATURE`: nếu mean < 223 → coi là °C và chuyển sang K.
- `PRESSURE`: Pa
- `HUMIDITY`: nếu mean > 1 → coi là % và chia 100 để về [0,1].

#### 2.3.1 Chuẩn hoá units end-to-end (raw → canonical → compute)

Để làm việc với nhiều nhà sản xuất (OEM) mà vẫn đảm bảo compute đúng, backend đã bổ sung **cấu hình units** tại model:

- `acquisition.ScadaUnitConfig`

Nguyên tắc:
- Dữ liệu raw (DB/file) được **convert về canonical units** trước khi chạy:
  - `preprocess_for_constants()` và ước lượng constants
  - `get_wpa()` (core compute)
- Canonical units trong compute:
  - `WIND_SPEED`: m/s
  - `ACTIVE_POWER`: kW
  - `TEMPERATURE`: K
  - `PRESSURE`: Pa (nếu raw pressure là `%/unknown` thì cột `PRESSURE` sẽ bị drop để density fallback)
  - `HUMIDITY`: ratio (0..1)

Lookup config theo thứ tự ưu tiên:
1) turbine + data_source (`db`/`file`)
2) turbine + `any`
3) farm + data_source
4) farm + `any`
5) global `any` (fallback default)

---

## 3) Constants đầu vào (`constants`)

Trong `smartWPA.required_constants` cần có:
- `V_cutin`, `V_cutout`, `V_rated` (m/s)
- `P_rated` (đơn vị cùng `ACTIVE_POWER` — canonical là kW)
- `Swept_area` (m²)

### 3.1 Ước lượng constants từ SCADA (mặc định luôn bật)

**Lưu ý quan trọng (theo thay đổi mới nhất)**:
- Hệ thống **không dùng DEFAULT_TURBINE_CONSTANTS cố định** nữa.
- Hệ thống **luôn** ước lượng `V_cutin/V_cutout/V_rated/P_rated` từ dữ liệu SCADA trước khi chạy `get_wpa()`.
- `Swept_area` là hằng số vật lý **không suy ra được từ SCADA**, nên mặc định:
  - `Swept_area = 20000 (m²)` nếu request không truyền.

Điểm gọi trong API:
- `api_gateway/turbines_analysis/computation.py`:
  - `df_for_constants = preprocess_for_constants(df.copy())`
  - `constants = derive_turbine_constants_from_scada(df_for_constants, base_constants={"Swept_area": ...})`
  - sau đó mới gọi `get_wpa(df, constants)`

### 3.1.1 Nguồn gốc và ý nghĩa từng hằng số

| Hằng số | Định nghĩa IEC 61400-12-1:2022 | Cách ước lượng trong SmartWPA | Lý do dùng công thức |
|--------|--------------------------------|--------------------------------|----------------------|
| **P_rated** | Rated power: công suất gán bởi nhà sản xuất cho điều kiện vận hành xác định (Clause 3.22). | Median của top 0,5% điểm có **P** ≥ 0. | Median giảm nhạy với spike/outlier; không phụ thuộc nameplate có thể thiếu trong SCADA. |
| **V_rated** | Không định nghĩa trực tiếp trong IEC; thường hiểu là tốc độ gió tại đó turbine đạt công suất định mức. | Bin 0,5 m/s nhỏ nhất mà **P̄_bin** ≥ 0,98·**P_rated** và **N_bin** ≥ 30. | 0,98 = “rated_alpha” theo config IEC-inspired; 30 điểm tương đương ≥ 5 giờ (10-min SCADA) đảm bảo thống kê ổn định. |
| **V_cutin** | IEC 3.5: “Lowest wind speed at which a wind turbine will begin to produce power”. | (1) Time-series: median tốc độ gió tại các transition 0 → P > 0,05·P_rated, duy trì ≥ 3 sample liên tiếp. (2) Fallback bin: bin nhỏ nhất **V** < **V_rated** với **P̄_bin** > 0,05·**P_rated**, **N_bin** ≥ 30. | 0,05 = “cutin_alpha”; time-series phản ánh hysteresis tốt hơn khi đủ dữ liệu (≥ ~4 tháng). |
| **V_cutout** | IEC 3.6: “Wind speed at which a wind turbine cuts out from the grid due to high wind speed”. | (1) Time-series: median tốc độ tại các sự kiện shutdown (P rated → ~0 trong ≤ 2 sample). (2) Fallback bin: bin nhỏ nhất **V** > **V_rated** với **P̄_bin** < 0,2·**P_rated** và tỉ lệ điểm “near-zero” (**P** < 0,02·**P_rated**) > 70%. | Phân biệt vùng cắt lưới do gió mạnh với curtailment; 70% near-zero xác nhận đây là shutdown thật. |

**Công thức chi tiết (code: `analytics/computation/constants_estimation.py`):**

- **P_rated** = median(top max(20, ⌈n·0,005⌉) điểm P ≥ 0), với **n** = số điểm dữ liệu. `prated_top_fraction = 0.005`, `prated_min_points = 20`.

- **V_rated** (bin 0,5 m/s, edges 0,25 — 0,75 — 1,25 — ... theo IEC-style):  
  V_rated = min{ v_i | P̄_i ≥ 0,98·P_rated và N_i ≥ 30 }.  
  Nếu không có bin nào thỏa: lấy bin có **P̄** lớn nhất.

- **V_cutin (bin-based)** = min{ v_i < V_rated | P̄_i > 0,05·P_rated và N_i ≥ 30 }.

- **V_cutout (bin-based)** = min{ v_i > V_rated | P̄_i < 0,2·P_rated, tỉ lệ điểm P < 0,02·P_rated > 70%, N_i ≥ 30 }.

**Bộ lọc sơ bộ trước khi ước lượng (IEC-inspired):**
- Tốc độ gió: chỉ giữ điểm trong [0, 32] m/s (32 m/s ~ tốc độ sinh tồn cực đoan cho phần lớn turbine — IEC 61400-50-1 / data quality).
- Công suất: [−500, 10000] kW (loại cảm biến lỗi thô trước khi biết P_rated).

### 3.2 Lưu constants đã ước lượng để trace (DB)

Mỗi lần chạy computation, các hằng số ước lượng được lưu vào `Computation` (DB) để truy vết:
- `Computation.v_cutin`
- `Computation.v_cutout`
- `Computation.v_rated`
- `Computation.p_rated`

Lưu ý: các hằng số này được gắn theo **turbine + computation_type + start_time/end_time**.

---

## 3.3 Overwrite/ghi đè dữ liệu khi chạy lại cùng khoảng thời gian

Khi chạy lại computation cho cùng turbine và cùng khoảng thời gian:
- `Computation` được `update_or_create(...)`
- Hệ thống đảm bảo chỉ có 1 record `is_latest=True` cho mỗi `(turbine, farm, computation_type)`
- Các bảng con sẽ bị **xóa trước khi lưu lại** để không bị “data cũ còn sót”:
  - classification: `ClassificationSummary`, `ClassificationPoint`
  - indicators: `IndicatorData`, `DailyProduction`, `CapacityFactorData`
  - power curve: `PowerCurveAnalysis`, `PowerCurveData`
  - weibull: `WeibullData`
  - yaw: `YawErrorData`, `YawErrorStatistics`

---

## 4) Preprocess dữ liệu (module `normalize.py`)

### 4.1 Làm sạch nhiệt độ
- Nếu mean(T) < 223 → `T = T + 273.15` (C → K)
- Nếu outlier \(T < 223K\) hoặc \(T > 323K\): set NaN và impute bằng `KNNImputer(n_neighbors=15)`

### 4.2 Làm sạch độ ẩm
- Nếu mean(H) > 1 → `H = H/100`
- Nếu outlier \(H<0\) hoặc \(H>1\): set NaN và impute bằng KNN

### 4.3 Làm sạch áp suất
- Nếu outlier \(P<50000\) hoặc \(P>108500\) Pa: set NaN và impute bằng KNN

---

## 5) Classification trạng thái vận hành (module `classifier.py`)

Output chính của classification là cột `status` thuộc tập:
`NORMAL`, `MEASUREMENT_ERROR`, `STOP`, `PARTIAL_STOP`,
`CURTAILMENT`, `PARTIAL_CURTAILMENT`,
`OVERPRODUCTION`, `UNDERPRODUCTION`, `UNKNOWN`.

### 5.0 Định nghĩa trạng thái và cơ sở tiêu chuẩn

Các trạng thái **không** lấy trực tiếp từ status code của turbine SCADA mà được **suy ra từ tín hiệu SCADA** (wind speed, active power) để phục vụ WPA (Wind Performance Analysis) và reliability. Cơ sở tham chiếu:

- **IEC 61400-12-1:2022** (Clause 8.4 Data rejection): loại bỏ dữ liệu khi turbine không vận hành bình thường, lỗi đo, ngoài sector, v.v. SmartWPA mở rộng thành **nhiều nhãn** để phân tích loss theo từng nguyên nhân.
- **IEC TS 61400-26** (availability/reliability): khái niệm UP/DOWN/OTHER; SmartWPA map UP = NORMAL + OVERPRODUCTION, DOWN = STOP, OTHER = các trạng thái còn lại (xem mục 12).

| Trạng thái | Định nghĩa trong SmartWPA | Cơ sở / Ghi chú | Mapping IEC |
|------------|---------------------------|------------------|--------------|
| **NORMAL** | Điểm nằm trong "healthy band" quanh đường cong công suất ước lượng (spline median). Band được tính adaptive từ dữ liệu thực tế của turbine tại site. | Band động từ dữ liệu, không dùng manufacturer curve (xem 5.8.4). Điểm NORMAL được dùng để tính power curve cuối cùng. | IEC TS 61400-26: **UP** |
| **OVERPRODUCTION** | Công suất đo > biên trên của healthy band. | Hiếm; có thể do đo đạc hoặc điều kiện đặc biệt (extreme wind conditions, measurement error). Không tính vào loss analysis. | IEC TS 61400-26: **UP** |
| **UNDERPRODUCTION** | Công suất đo < biên dưới của healthy band (nhưng không phải CURTAILMENT). | Mất hiệu suất so với curve "khỏe". Có thể do: aging, icing, yaw misalignment, partial fault, etc. Tính vào loss analysis. | IEC TS 61400-26: **OTHER** |
| **STOP** | ACTIVE_POWER ≤ 0 (theo rule). Cuối cùng mọi điểm P ≤ 0 đều bị gán STOP. | Tương ứng "turbine not operating" trong IEC 8.4. Tính vào downtime, MTTR/MTTF. | IEC 8.4: **Reject** (turbine not operating)<br>IEC TS 61400-26: **DOWN** |
| **PARTIAL_STOP** | Các đoạn liền kề trước/sau một chuỗi STOP dài (≥ 240 phút), nếu đoạn đó chỉ gồm NORMAL/UNDER/OVER và có độ dài ≥ 40 phút. | Heuristic để đánh dấu vùng "ảnh hưởng" bởi dừng máy (cooling, startup sequence, etc.). Xem 5.8.6. | IEC TS 61400-26: **OTHER** |
| **CURTAILMENT** | UNDERPRODUCTION đồng thời công suất ổn định (rolling std 30 phút < 100 kW) và duration ≥ 30 phút. | Giả định curtailment tạo plateau/setpoint (power constant). Phân biệt với underproduction do fault (có variation). Xem 5.8.5. | IEC TS 61400-26: **OTHER** |
| **PARTIAL_CURTAILMENT** | Đoạn "normal" (không phải CURTAILMENT) nằm sát trước/sau một nhóm CURTAILMENT, đủ dài (≥ 40 phút). | Vùng transition/ramp-up sau curtailment. Xem 5.8.6. | IEC TS 61400-26: **OTHER** |
| **MEASUREMENT_ERROR** | Dữ liệu ngoài khoảng hợp lệ, NaN, hoặc vi phạm rule vật lý (xem 5.1). Bao gồm: V ngoài [0,32] m/s, P ngoài [-0.05·P_rated, 1.1·P_rated], NaN, V ngoài [V_cutin-1, V_cutout+1] khi P>0, \|ΔV\|>10 m/s, wind "đứng yên" ≥1h. | Tương ứng "failure or degradation of measurement equipment" (IEC 8.4d). Loại bỏ khỏi power curve và loss analysis. | IEC 8.4d: **Reject** (measurement equipment failure)<br>IEC TS 61400-26: **OTHER** |
| **UNKNOWN** | Ban đầu gán cho điểm chưa bị loại ở Bước A; sau các bước B–G được chuyển thành NORMAL/UNDER/OVER/CURTAILMENT/… | Trạng thái trung gian trong pipeline. Không nên còn UNKNOWN sau khi hoàn thành classification. | IEC TS 61400-26: **OTHER** |

**Hằng số thời gian (code: `classifier.py`):**
- `TIME_RESOLUTION = 10` phút.
- `LEAST_TIME_OF_CURTAILMENT = 30` phút (tối thiểu để gán CURTAILMENT).
- `LEAST_TIME_OF_STOP = 240` phút (tối thiểu để coi là “stop group” khi gán PARTIAL_STOP).
- `LEAST_TIME_OF_NORMAL = 40` phút (tối thiểu đoạn “normal” khi tìm PARTIAL_*).
- `MAX_DIFF_IN_CURTAILMENT = 100` (rolling std của power < 100 để coi là “ổn định” curtailment).

### 5.1 Bước A — Filter lỗi đo (`filter_error`)
Gán `status` ban đầu theo rule:
- Nếu `ACTIVE_POWER <= 0` → `STOP`
- Nếu `WIND_SPEED < 0` → `MEASUREMENT_ERROR`
- Mặc định còn lại → `UNKNOWN`

Sau đó “siết” thêm `MEASUREMENT_ERROR` theo các điều kiện:
- Wind speed ngoài [0, 32] m/s
- Power ngoài [-0,05·**P_rated**, 1,1·**P_rated**]
- `WIND_SPEED` hoặc `ACTIVE_POWER` bị NaN
- Nếu **V** < **V_cutin** − 1 hoặc **V** > **V_cutout** + 1 mà `ACTIVE_POWER > 0`
- Nếu |**ΔV**| > 10 m/s so với sample trước → set status error và set `WIND_SPEED`, `ACTIVE_POWER` = NaN
- Nếu trong tối thiểu 1 giờ (>= 6 điểm 10 phút) wind speed “đứng yên” → mark error

### 5.2 Bước B — Lọc outlier (DBSCAN)
Trên phần dữ liệu `status == UNKNOWN`, chạy DBSCAN (feature: `WIND_SPEED`, `ACTIVE_POWER`) với:
- `min_samples=15`
- `eps=0.2` (đã “hardcode” khi gọi)

Mục tiêu: lấy tập “inlier” để fit đường cong “healthy”.

### 5.3 Bước C — Regression power curve thô (`power_curve_regression`)
Chia bin theo `bin_width = 0.25` m/s, và mỗi bin lấy:
- `center` = mean wind speed trong bin
- `median` = median active power trong bin

Tạo bảng curve, rồi forward-fill/back-fill để không bị gap.

### 5.4 Bước D — “Healthy band” và phân lớp performance
Trên dữ liệu, nội suy spline:
- **P_theoretical(V)** = CubicSpline(center, median)

Sau đó tìm biên dưới/trên theo sai lệch:
- **lower_dev** = P_theoretical − P_measured (khi thiếu công suất)
- **upper_dev** = P_measured − P_theoretical (khi dư công suất)

Band được chọn bằng cách quét ngưỡng độ lệch theo step (10) đến max_band (1000) và chọn ngưỡng nơi “tốc độ tăng” số điểm mới trong band nhỏ hơn `stop_threshold=0.002`.

Cuối cùng:
- Nếu `ACTIVE_POWER < lower` → `UNDERPRODUCTION`
- Nếu `ACTIVE_POWER > upper` → `OVERPRODUCTION`
- Ngược lại → `NORMAL`
(áp dụng cho các điểm còn `UNKNOWN`).

### 5.5 Bước E — Curtailment
Định nghĩa “curtailment candidate”:
- status đang là `UNDERPRODUCTION`
- rolling std của `ACTIVE_POWER` trong cửa sổ 30 phút nhỏ hơn 100

Các chuỗi candidate liên tục có duration >= 30 phút được gán `CURTAILMENT`.

### 5.6 Bước F — Partial curtailment
Với mỗi “curtailment group”, tìm các “normal group” gần nhất trước/sau có độ dài tối thiểu 40 phút.
Nếu đoạn trước/sau không chứa `CURTAILMENT` thì gán `PARTIAL_CURTAILMENT`.

### 5.7 Bước G — Partial stop
Tìm các chuỗi `STOP` liên tục có duration >= 240 phút.  
Sau đó, các đoạn trước/sau gần nhất (>= 40 phút) nếu chỉ gồm `NORMAL/UNDERPRODUCTION/OVERPRODUCTION` thì gán `PARTIAL_STOP`.

Cuối cùng, mọi điểm có `ACTIVE_POWER <= 0` được "force" về `STOP`.

---

### 5.8 Đối chiếu với IEC 61400-12-1:2022 và lý do lựa chọn

#### 5.8.1 Data rejection (Bước A) - So sánh với IEC 8.4

**IEC 61400-12-1:2022 Clause 8.4** liệt kê các điều kiện loại bỏ dữ liệu:
- Turbine không vận hành bình thường (not in normal operation)
- Fault conditions
- Manual shutdown
- Test/maintenance periods
- Failure or degradation of measurement equipment
- Wind direction ngoài measurement sector
- Data outside valid measurement range

**SmartWPA approach** (`filter_error` function):
- **Khác biệt chính**: SmartWPA **không dùng status code từ turbine SCADA** mà **suy ra từ tín hiệu SCADA** (wind speed, active power). Lý do:
  1. Status code có thể không đáng tin cậy hoặc không có sẵn trong dữ liệu lịch sử
  2. Phương pháp dựa trên SCADA signals cho phép phân tích dữ liệu cũ mà không cần status code
  3. Tự động phát hiện lỗi đo từ dữ liệu vật lý (physical constraints)

**Các tiêu chí cụ thể và lý do**:

| Tiêu chí | Giá trị | Lý do lựa chọn | So với IEC |
|----------|---------|----------------|------------|
| Wind speed range | [0, 32] m/s | 32 m/s là giới hạn hợp lý cho hầu hết turbine (cut-out thường 20-25 m/s, thêm margin cho extreme events). Giá trị < 0 là vật lý không hợp lệ. | **Phù hợp**: IEC yêu cầu loại dữ liệu ngoài range hợp lệ, nhưng không chỉ định cụ thể 32 m/s. Giá trị này dựa trên kinh nghiệm thực tế. |
| Power range | [-0.05·P_rated, 1.1·P_rated] | -0.05·P_rated cho phép noise/measurement error nhỏ. 1.1·P_rated cho phép overshoot tạm thời (transient) nhưng loại bỏ spike bất thường. | **Phù hợp**: IEC yêu cầu loại dữ liệu ngoài range, ngưỡng này hợp lý cho SCADA data. |
| NaN detection | WIND_SPEED hoặc ACTIVE_POWER = NaN | Dữ liệu thiếu không thể dùng cho phân tích. | **Đúng**: Tương ứng IEC 8.4d (failure of measurement equipment). |
| V ngoài [V_cutin-1, V_cutout+1] khi P>0 | Margin ±1 m/s | Nếu có power nhưng wind speed ngoài operating range (có margin), có thể là lỗi đo hoặc dữ liệu không hợp lệ. | **Mở rộng hợp lý**: IEC đề cập cut-out hysteresis nhưng không chỉ định margin cụ thể. Margin 1 m/s tránh false positive do uncertainty trong ước lượng V_cutin/V_cutout. |
| |ΔV| > 10 m/s | Wind speed không thể thay đổi đột ngột > 10 m/s trong 10 phút (giới hạn vật lý). | **Heuristic hợp lý**: IEC không chỉ định ngưỡng này, nhưng dựa trên kiến thức vật lý về turbulence và wind speed variation. 10 m/s là ngưỡng conservative để phát hiện spike/sensor error. |
| Wind speed "đứng yên" ≥ 1 giờ | 6 điểm liên tiếp (10 phút/điểm) | Anemometer bị "stuck" hoặc dead value. Trong điều kiện thực tế, wind speed luôn có variation. | **Heuristic hợp lý**: IEC không chỉ định cụ thể, nhưng đây là dấu hiệu rõ ràng của sensor failure. 1 giờ đủ dài để loại bỏ false positive (có thể có gió yếu nhưng vẫn có variation nhỏ). |

**Kết luận về Bước A**:
- ✅ **Phù hợp với tinh thần IEC 8.4**: Loại bỏ dữ liệu không hợp lệ và lỗi đo
- ✅ **Mở rộng hợp lý**: Dùng SCADA signals thay vì status code, phù hợp với dữ liệu lịch sử
- ✅ **Các ngưỡng hợp lý**: Dựa trên vật lý và kinh nghiệm thực tế

#### 5.8.2 Các trạng thái mở rộng - Lý do và cơ sở

IEC 61400-12-1:2022 chỉ có khái niệm **accept/reject** dữ liệu. SmartWPA mở rộng thành **nhiều nhãn** để phục vụ WPA và reliability analysis.

| Trạng thái | Có trong IEC? | Lý do mở rộng | Ứng dụng |
|------------|---------------|---------------|----------|
| **MEASUREMENT_ERROR** | ✅ Có (8.4d) | Tương ứng "failure or degradation of measurement equipment". | Loại bỏ khỏi power curve, không tính vào loss analysis. |
| **STOP** | ✅ Có (8.4 - turbine not operating) | Tương ứng "turbine not in normal operation". | Tính loss do downtime, MTTR/MTTF cho reliability. |
| **NORMAL** | ❌ Không | IEC chỉ có accept/reject. SmartWPA cần nhãn "normal" để phân biệt với các trạng thái khác. | Dùng để tính power curve (chỉ dùng NORMAL points), baseline cho performance analysis. |
| **UNDERPRODUCTION** | ❌ Không | IEC không phân biệt mức độ performance. SmartWPA cần để phân tích loss do performance degradation. | Tính loss energy, phân tích nguyên nhân (aging, icing, etc.). |
| **OVERPRODUCTION** | ❌ Không | Hiếm nhưng có thể xảy ra (measurement error, extreme conditions). | Đánh dấu để review, không tính vào loss. |
| **CURTAILMENT** | ❌ Không | IEC không phân biệt curtailment vs. other underproduction. SmartWPA cần để phân tích loss do grid constraints. | Tính curtailment loss riêng, phân tích tác động của grid limits. |
| **PARTIAL_CURTAILMENT** | ❌ Không | Heuristic để đánh dấu vùng transition/ramp-up sau curtailment. | Phân tích chi tiết hơn về tác động của curtailment. |
| **PARTIAL_STOP** | ❌ Không | Heuristic để đánh dấu vùng transition/ramp-up sau stop. | Phân tích chi tiết hơn về tác động của downtime. |

**Cơ sở cho mở rộng**:
- **IEC TS 61400-26** (Availability classification): Định nghĩa UP/DOWN/OTHER states. SmartWPA mapping:
  - UP = NORMAL + OVERPRODUCTION
  - DOWN = STOP
  - OTHER = UNDERPRODUCTION, CURTAILMENT, PARTIAL_*, MEASUREMENT_ERROR
- **WPA requirements**: Cần phân tích loss theo từng nguyên nhân để tối ưu hóa performance
- **Industry practice**: Nhiều hệ thống WPA commercial (như WindPRO, Openwind) cũng có classification tương tự

**Kết luận**: Mở rộng này **hợp lý và cần thiết** cho WPA, không vi phạm IEC 61400-12-1 (vì IEC chỉ định nghĩa data rejection, không định nghĩa classification states).

#### 5.8.3 DBSCAN outlier filtering - Tại sao cần thiết

**IEC không có tương đương**: IEC 8.4 chỉ loại bỏ dữ liệu rõ ràng sai (out of range, NaN, etc.), không có bước lọc outlier dựa trên clustering.

**Lý do SmartWPA cần DBSCAN**:
1. **Vấn đề**: Sau Bước A, vẫn còn nhiều điểm "UNKNOWN" có thể là:
   - Outlier do nhiễu (noise)
   - Data từ các operating mode khác (startup, shutdown, fault recovery)
   - Measurement errors không bị phát hiện ở Bước A

2. **Tại sao không dùng phương pháp khác**:
   - **Statistical outlier (Z-score, IQR)**: Không hiệu quả với dữ liệu 2D (wind speed, power) có phân phối không đều
   - **Isolation Forest**: Tốn tài nguyên, không cần thiết cho dữ liệu 2D
   - **DBSCAN**: 
     - ✅ Phù hợp với dữ liệu 2D có cụm (clusters)
     - ✅ Tự động xác định số cụm (không cần biết trước)
     - ✅ Phát hiện outlier hiệu quả (điểm không thuộc cụm nào)
     - ✅ Robust với noise

3. **Tham số lựa chọn**:
   - `min_samples=15`: Tương đương 2.5 giờ dữ liệu (15 × 10 phút). Đủ để tạo cụm đáng tin cậy, không quá lớn để bỏ sót cụm nhỏ.
   - `eps=0.2`: Sau khi standardize (mean=0, std=1), 0.2 là khoảng cách hợp lý giữa các điểm trong cùng cụm. Được chọn dựa trên thực nghiệm.

4. **Kết quả**: DBSCAN tách được "inlier cluster" (điểm healthy) khỏi outlier, giúp fit healthy curve chính xác hơn.

**Kết luận**: DBSCAN là **bước cần thiết** để đảm bảo healthy curve được fit từ dữ liệu chất lượng cao, mặc dù không có trong IEC.

#### 5.8.4 Healthy band approach - Tại sao không dùng manufacturer curve

**IEC không có khái niệm "healthy band"**: IEC chỉ định nghĩa measured power curve (từ dữ liệu), không có khái niệm band để phân loại performance.

**Lý do không dùng manufacturer curve**:
1. **Manufacturer curve không phù hợp với site cụ thể**:
   - Manufacturer curve được đo ở điều kiện chuẩn (standard conditions)
   - Site thực tế có điều kiện khác (terrain, wake effects, air density, etc.)
   - Dùng manufacturer curve sẽ tạo nhiều false positive (UNDERPRODUCTION khi thực tế là normal cho site đó)

2. **Adaptive healthy band phù hợp hơn**:
   - Band được tính từ dữ liệu thực tế của turbine tại site
   - Phản ánh đúng performance "khỏe" của turbine tại site cụ thể
   - Tự động điều chỉnh theo điều kiện site (không cần calibration)

3. **Cách tính healthy band**:
   - Bắt đầu từ median curve (robust hơn mean)
   - Quét band từ 10 đến 1000 kW với step 10 kW
   - Chọn band nơi "tốc độ tăng" điểm mới < 0.2% (stop_threshold=0.002)
   - Lý do: Band quá nhỏ → quá strict, band quá lớn → không phân biệt được underproduction. Stop khi tốc độ tăng điểm mới chậm → đã đạt "natural boundary" của healthy operation.

4. **Ưu điểm**:
   - ✅ Tự động, không cần manual calibration
   - ✅ Phù hợp với từng turbine/site
   - ✅ Robust với variation tự nhiên của wind/power

**Kết luận**: Healthy band approach là **lựa chọn đúng đắn** cho WPA, phù hợp hơn manufacturer curve cho phân tích performance thực tế.

#### 5.8.5 Curtailment detection - Heuristic và ngưỡng

**IEC không có định nghĩa curtailment**: IEC chỉ loại bỏ dữ liệu, không phân biệt curtailment vs. other underproduction.

**Lý do cần phát hiện curtailment**:
- Curtailment là loss do grid constraints (không phải lỗi turbine)
- Cần tách riêng để phân tích: loss do turbine vs. loss do grid
- Quan trọng cho energy forecasting và grid planning

**Heuristic phát hiện curtailment**:
1. **Điều kiện**: UNDERPRODUCTION + công suất ổn định
   - **UNDERPRODUCTION**: Đã được xác định từ healthy band
   - **Ổn định**: Rolling std (30 phút) < 100 kW
     - Lý do: Curtailment tạo "plateau" (setpoint cố định), khác với underproduction do fault (có variation)

2. **Duration threshold**: ≥ 30 phút
   - Lý do: Tránh false positive từ transient ngắn
   - 30 phút đủ dài để phân biệt curtailment (thường kéo dài) vs. transient

3. **Stability threshold**: std < 100 kW
   - Lý do: Với 10 phút resolution, std < 100 kW nghĩa là power gần như constant
   - Ngưỡng này phù hợp với hầu hết turbine (P_rated thường 2-5 MW, variation < 100 kW là rất nhỏ)

**Kết luận**: Heuristic này **hợp lý và hiệu quả** để phát hiện curtailment từ SCADA data, mặc dù không có trong IEC.

#### 5.8.6 Partial states - Lý do và ứng dụng

**IEC không có khái niệm partial states**: IEC chỉ có accept/reject, không có trạng thái "partial".

**Lý do cần partial states**:
1. **PARTIAL_STOP**: Đánh dấu vùng transition/ramp-up sau stop
   - Lý do: Sau khi turbine dừng lâu (≥ 240 phút), vùng trước/sau có thể bị ảnh hưởng (cooling, startup sequence, etc.)
   - Ứng dụng: Phân tích chi tiết hơn về tác động của downtime

2. **PARTIAL_CURTAILMENT**: Đánh dấu vùng transition/ramp-up sau curtailment
   - Lý do: Sau khi curtailment kết thúc, turbine cần thời gian để ramp-up về normal
   - Ứng dụng: Phân tích chi tiết hơn về tác động của grid constraints

**Heuristic**:
- Tìm các "normal group" gần nhất (trước/sau) có độ dài ≥ 40 phút
- Lý do 40 phút: Đủ dài để đảm bảo đây là "real normal operation", không phải transient ngắn
- Chỉ gán nếu đoạn không chứa CURTAILMENT/STOP (tránh overlap)

**Kết luận**: Partial states là **heuristic hợp lý** để phân tích chi tiết hơn, mặc dù không có trong IEC.

---

### 5.9 Các hằng số và ngưỡng - Giải thích chi tiết

#### 5.9.1 Hằng số thời gian

| Hằng số | Giá trị | Lý do lựa chọn | Cơ sở |
|---------|---------|----------------|-------|
| `TIME_RESOLUTION` | 10 phút | Resolution chuẩn của SCADA data. Đủ fine để capture dynamics, đủ coarse để giảm noise. | Industry standard (hầu hết SCADA systems dùng 10 phút). |
| `LEAST_TIME_OF_CURTAILMENT` | 30 phút | Tối thiểu để phân biệt curtailment (kéo dài) vs. transient ngắn. | Thực nghiệm: curtailment thường kéo dài ≥ 30 phút. Transient thường < 30 phút. |
| `LEAST_TIME_OF_STOP` | 240 phút (4 giờ) | Tối thiểu để coi là "stop event" nghiêm trọng (không phải brief shutdown). | Reliability analysis: downtime < 4 giờ thường là brief shutdown, không phải major failure. |
| `LEAST_TIME_OF_NORMAL` | 40 phút | Tối thiểu để đảm bảo "real normal operation" (không phải transient). | Thực nghiệm: 40 phút đủ dài để loại bỏ transient, đủ ngắn để không bỏ sót normal periods. |

#### 5.9.2 Ngưỡng phát hiện lỗi

| Ngưỡng | Giá trị | Lý do lựa chọn | Cơ sở |
|--------|---------|----------------|-------|
| Wind speed range | [0, 32] m/s | 32 m/s là giới hạn hợp lý (cut-out thường 20-25 m/s + margin). | Kinh nghiệm: hầu hết turbine có cut-out < 30 m/s. 32 m/s là conservative upper bound. |
| Power range | [-0.05·P_rated, 1.1·P_rated] | -0.05 cho phép noise nhỏ. 1.1 cho phép overshoot tạm thời. | Thực nghiệm: power có thể có noise ±5% và overshoot tạm thời đến 110%. |
| |ΔV| threshold | 10 m/s | Wind speed không thể thay đổi > 10 m/s trong 10 phút (giới hạn vật lý). | Vật lý: turbulence và wind speed variation tự nhiên < 10 m/s trong 10 phút. |
| Dead value detection | 1 giờ (6 điểm) | Anemometer bị stuck. Trong điều kiện thực tế, wind speed luôn có variation. | Thực nghiệm: 1 giờ đủ dài để loại bỏ false positive (có thể có gió yếu nhưng vẫn có variation nhỏ). |

#### 5.9.3 Ngưỡng phân loại performance

| Ngưỡng | Giá trị | Lý do lựa chọn | Cơ sở |
|--------|---------|----------------|-------|
| `MAX_DIFF_IN_CURTAILMENT` | 100 kW | Rolling std < 100 kW nghĩa là power gần như constant (plateau). | Thực nghiệm: với P_rated 2-5 MW, variation < 100 kW là rất nhỏ, phù hợp với curtailment setpoint. |
| `bin_width` (Bước C) | 0.25 m/s | Fine hơn IEC 0.5 m/s để tạo curve mượt hơn cho classification. | Lý do: curve này dùng để phân loại, không phải power curve cuối cùng. 0.25 m/s cho độ phân giải tốt hơn. |
| `step_size` (healthy band) | 10 kW | Step đủ nhỏ để tìm band chính xác, đủ lớn để không quá chậm. | Thực nghiệm: 10 kW là balance tốt giữa accuracy và performance. |
| `stop_threshold` | 0.002 (0.2%) | Tốc độ tăng điểm mới < 0.2% → đã đạt natural boundary. | Thực nghiệm: < 0.2% nghĩa là band đã đủ lớn, tăng thêm không có ý nghĩa. |
| `max_band` | 1000 kW | Giới hạn trên cho healthy band (tránh band quá lớn). | Thực nghiệm: band > 1000 kW không còn ý nghĩa phân biệt performance. |

#### 5.9.4 Tham số DBSCAN

| Tham số | Giá trị | Lý do lựa chọn | Cơ sở |
|---------|---------|----------------|-------|
| `min_samples` | 15 | Tương đương 2.5 giờ dữ liệu. Đủ để tạo cụm đáng tin cậy. | Thực nghiệm: 15 điểm đủ để tạo cụm, không quá lớn để bỏ sót cụm nhỏ. |
| `eps` | 0.2 | Sau standardize, 0.2 là khoảng cách hợp lý giữa các điểm trong cùng cụm. | Thực nghiệm: được chọn dựa trên phân phối dữ liệu sau khi standardize. |

---

### 5.10 Mapping với IEC TS 61400-26 (Availability)

**IEC TS 61400-26** định nghĩa availability classification với 3 states:
- **UP**: Turbine available for power production
- **DOWN**: Turbine not available for power production
- **OTHER**: States that are neither UP nor DOWN (transitional, reduced availability, etc.)

**SmartWPA mapping** (dùng trong `indicators.py` và `reliability.py`):

| SmartWPA Status | IEC TS 61400-26 | Lý do |
|-----------------|-----------------|-------|
| `NORMAL` | **UP** | Turbine đang vận hành bình thường, available for production. |
| `OVERPRODUCTION` | **UP** | Turbine đang vận hành (có thể do điều kiện đặc biệt), vẫn available. |
| `STOP` | **DOWN** | Turbine không phát điện, not available. |
| `UNDERPRODUCTION` | **OTHER** | Turbine đang vận hành nhưng performance giảm (có thể do fault, aging, etc.). Không phải UP (vì không đạt performance) nhưng cũng không phải DOWN (vì vẫn đang chạy). |
| `CURTAILMENT` | **OTHER** | Turbine bị giới hạn bởi grid constraints. Không phải UP (vì không đạt full potential) nhưng cũng không phải DOWN (vì vẫn đang chạy). |
| `PARTIAL_CURTAILMENT` | **OTHER** | Transitional state, không phải UP/DOWN rõ ràng. |
| `PARTIAL_STOP` | **OTHER** | Transitional state sau stop, không phải UP/DOWN rõ ràng. |
| `MEASUREMENT_ERROR` | **OTHER** | Không thể xác định state thực tế, không tính vào UP/DOWN. |
| `UNKNOWN` | **OTHER** | Không xác định được, không tính vào UP/DOWN. |

**Lý do một số trạng thái được xếp vào OTHER**:
- **UNDERPRODUCTION**: Turbine vẫn đang chạy nhưng không đạt performance. Nếu xếp vào DOWN → sai (vì turbine không down). Nếu xếp vào UP → sai (vì không đạt full potential). → **OTHER** là hợp lý.
- **CURTAILMENT**: Tương tự, turbine đang chạy nhưng bị giới hạn bởi external factors (grid). → **OTHER**.
- **PARTIAL_***: Transitional states, không rõ ràng UP/DOWN. → **OTHER**.

**Ứng dụng**:
- **TBA (Time-based Availability)**: TBA = UP / (UP + DOWN)
- **PBA (Production-based Availability)**: PBA = RealEnergy / ReachableEnergy (chỉ tính trên UP states)
- **MTTR/MTTF/MTBF**: Chỉ tính trên UP ↔ DOWN transitions

**Kết luận**: Mapping này **phù hợp với IEC TS 61400-26**, cho phép tính availability metrics chuẩn từ classification states của SmartWPA.

---

## 6) Air density (module `density.py`)

Nếu có đủ `HUMIDITY`, `TEMPERATURE`, `PRESSURE` thì dùng dữ liệu theo thời gian, nếu không thì dùng hằng số trong `_header.py` hoặc fallback `AIR_DENSITY = 1.225` kg/m³.

**Nguồn công thức:** IEC 61400-12-1:2022, Clause 9.1.5, Equation (12). Mật độ không khí được tính từ nhiệt độ, áp suất và độ ẩm tương đối.

Hàm `calculate_air_density(temp, pressure, humidity)` implement:

- **ρ** = (1/**T**) · [ **p**/**R_air** − **h** · 0,0631846 · **T** · (1/**R_air** − 1/**R_vapor**) ]

**Ý nghĩa các đại lượng:**
- **T**: nhiệt độ tuyệt đối [K]. Dữ liệu °C được chuyển sang K trong `normalize.py` (nếu mean < 223).
- **p**: áp suất không khí tại hub height [Pa]. IEC yêu cầu hiệu chỉnh về hub height theo ISO 2533 nếu sensor không ở hub.
- **h**: độ ẩm tương đối dạng ratio (0..1). Nếu dữ liệu là % thì chia 100.
- **R_air** = 287,05 J/(kg·K) — hằng số khí của không khí khô (IEC ký hiệu R₀).
- **R_vapor** = 461,5 J/(kg·K) — hằng số khí của hơi nước (IEC ký hiệu R_w).
- Số **0,0631846** xuất hiện trong biểu thức áp suất hơi nước. IEC 9.1.5: **P_w** = 0,0000205·exp(0,0631846·T) [Pa]. Trong code hiện tại dùng dạng tuyến tính **h · 0,0631846 · T** thay cho **Φ·P_w** (với **Φ** là độ ẩm 0–100%); điều này gần đúng trong vùng nhiệt độ thường dùng nhưng khác với IEC ở dạng hàm.

**Fallback:** `_header.py` định nghĩa `AIR_DENSITY = 1.225` kg/m³ (giá trị tiêu chuẩn ở điều kiện sea level, 15°C).

---

## 7) Normalize về mật độ khí chuẩn 1.225 kg/m³ (module `normalize.py`)

**Nguồn:** IEC 61400-12-1:2022, Clause 9.1.5. Tiêu chuẩn quy định chuẩn hóa về reference air density (trung bình đo tại site hoặc 1.225 kg/m³). Với turbine **điều khiển công suất chủ động** (active power control), chuẩn hóa áp dụng cho **tốc độ gió** theo Equation (14); với turbine stall-regulated thì chuẩn hóa **công suất** theo Equation (13). SmartWPA dùng công thức cho active power control (wind speed normalization).

Áp dụng trên tập **NORMAL** (sau classification), trước khi binning/power curve:

- **Normalize wind speed** (IEC Eq. 14): **V_norm** = **V** · (**ρ**/**ρ₀**)^(1/3), với **ρ₀** = 1,225 kg/m³.  
  Lý do: công suất tỉ lệ với **ρ·V³** (động năng qua rotor); giữ **P/ρ** không đổi khi đổi **ρ** thì **V** tỉ lệ **ρ**^(−1/3).

- **Normalize power** (IEC Eq. 13 cho stall; SmartWPA vẫn dùng để đưa P về điều kiện chuẩn): **P_norm** = **P** · (**ρ₀**/**ρ**).

---

## 8) Binning wind speed (module `bins.py`)

**Nguồn:** IEC 61400-12-1:2022, Clause 8.5 Database và 9.2. “Method of bins”: wind speed range chia thành các bin **0,5 m/s liên tiếp**, tâm tại bội số của 0,5 m/s.

Chia bin độ rộng **0,5 m/s** theo:
- Edges: 0,25; 0,75; 1,25; ...
- Labels (center): 0,5; 1,0; 1,5; ...

`bin` được lưu dạng float (center của bin). Cùng scheme được dùng trong `constants_estimation._iec_bin_centers` để thống nhất với power curve và AEP.

---

## 9) Power curve (module `curve_est.py`)

**Nguồn:** IEC 61400-12-1:2022, Clause 9.2. Measured power curve xác định bằng “method of bins” trên dữ liệu đã chuẩn hóa: với mỗi bin \(i\), trung bình normalized wind speed và normalized power (IEC Eqs 15, 16).

### 9.1 Power curve “global”
Trên data đã bin (sau khi normalize theo **ρ**):

- **P_i** = (1/**N_i**) Σ (trong bin i) P_{n,j}
- **V_i** = (1/**N_i**) Σ (trong bin i) V_{n,j}

Trong pipeline chính, input của power curve là **data NORMAL** nên thực tế curve global là curve của NORMAL. Code dùng mean power theo bin (tương đương IEC).

### 9.2 Power curve theo thời gian
Ngoài “global”, code xuất thêm:
- `yearly`: group theo `YS` (start of year)
- `quarterly`: group theo quý
- `monthly`: group theo tháng
- `day/night`: phân nhóm giờ (Night nếu hour >= 18 hoặc < 6)

---

## 10) Ước lượng công suất lý thuyết (module `estimate.py` + `func_est.py`)

`estimate(fill_flag=True)`:
- Nội suy tuyến tính theo time cho `WIND_SPEED`, `ACTIVE_POWER` (pandas `interpolate(method='time')`)
- Fit mô hình Logistic 5 tham số trên tập `status == NORMAL`

Hàm 5PL: **f(x)** = **D** + (**A**−**D**) / (1 + (x/**C**)^**B**)^**E**

Ước lượng tham số bằng `scipy.optimize.curve_fit`, rồi dự báo:
- `ESTIMATED_POWER = f(WIND_SPEED)`

---

## 11) Energy-based KPIs & thống kê (module `indicators.py`)

Ký hiệu:
- **P_i**: `ACTIVE_POWER` tại sample i
- **P̂_i**: `ESTIMATED_POWER` tại sample i
- **Δt**: time step (giờ) = resolution / 1 hour

### 11.1 Average wind speed
- **AverageWindSpeed** = mean(V_i)

### 11.2 ReachableEnergy / RealEnergy
- **E_reachable** = Σ_i **P̂_i** · **Δt**
- **E_real** = Σ_i **P_i** · **Δt**

### 11.3 LossEnergy / LossPercent
- **E_loss** = max(0, **E_reachable** − **E_real**)
- **LossPercent** = **E_loss** / **E_reachable** nếu E_reachable > 0; bằng 0 nếu ngược lại

### 11.4 DailyProduction
Group theo ngày (freq `'D'`):
- **E_day** = Σ (i ∈ ngày) **P_i** · **Δt**  

Output là list record `{date, DailyProduction}`.

### 11.5 Tba (Time-based availability, theo code)
Theo code:
- **R** = số samples thuộc một trong: `NORMAL`, `CURTAILMENT`, `PARTIAL_CURTAILMENT`, `OVERPRODUCTION`, `UNDERPRODUCTION`
- **U** = số samples thuộc: `STOP`, `PARTIAL_STOP`

- **Tba** = **R** / (**R** + **U**)

### 11.6 Pba (Production-based availability, theo code)
Loại `MEASUREMENT_ERROR`:
- **Pba** = (Σ **P_i**) / (Σ **P̂_i**), với i trên subset `status != MEASUREMENT_ERROR`.

### 11.7 Loss theo từng trạng thái
Với mỗi trạng thái **S** trong: `STOP`, `PARTIAL_STOP`, `UNDERPRODUCTION`, `CURTAILMENT`, `PARTIAL_CURTAILMENT`:

- **Loss_S** = max(0, Σ_{i∈S} (**P̂_i** − **P_i**) · **Δt**)

### 11.8 Thống kê counts & durations (theo code)
- `TotalStopPoints` = số điểm `status == STOP` (từ `classified`)
- Tương tự cho `PARTIAL_STOP`, `UNDERPRODUCTION`, `CURTAILMENT`
- `TimeStep` = **Δt** theo giây
- `TotalDuration` = (max timestamp - min timestamp) theo giây

### 11.9 Yaw error histogram (nếu đủ cột)
`yaw_error.yaw_errors()`:
- **Δ** = θ_nacelle − θ_wind (hoặc dùng cột yaw có sẵn nếu có)
- Normalize về [−180, 180)
- Histogram theo bin 10° (mặc định; có thể truyền qua constants `yaw_bin_width`: 5, 10, 15)
- Lọc theo **cut-in/cut-out** (V_cutin, V_cutout từ constants) khi có cột WIND_SPEED
- Trả mean/median/std của \(\Delta\); API trả thêm alias `yaw_misalignment` = mean, `yaw_lag` = std

**Constants tùy chọn (khi gọi computation):**
- `yaw_bin_width`: 5 | 10 | 15 (bước histogram, độ)
- `yaw_only_computed_states`: list trạng thái classification để giữ (vd. `['NORMAL']`); `None` = dùng mọi điểm
- `yaw_precomputed_column`: tên cột SCADA chứa yaw error sẵn (degrees)
- `yaw_months`, `yaw_day_night` ('day'|'night'), `yaw_direction_sector_deg`, `yaw_direction_sectors`, `yaw_source_filters`: advanced filters (manual)

---

## 12) Reliability KPIs (IEC TS 61400-26-4 “inspired”, strict) — module `reliability.py`

### 12.1 Mapping trạng thái (đang dùng trong `indicators.py`)
- **UP**: `NORMAL`, `OVERPRODUCTION`
- **DOWN**: `STOP`
- **OTHER/ignored**: `PARTIAL_STOP`, `CURTAILMENT`, `PARTIAL_CURTAILMENT`, `UNDERPRODUCTION`, `MEASUREMENT_ERROR`, `UNKNOWN`

### 12.2 Failure event
**Failure event** được định nghĩa là mỗi lần chuyển **UP → DOWN**, và các sample DOWN liên tiếp được gộp thành 1 interval.

Quy tắc quan trọng theo code:
- Các sample `OTHER` **không** mở/đóng event và **không** làm thay đổi “last meaningful state”.
- Khi gặp UP sau DOWN, event đóng tại `ts - dt` (timestamp DOWN cuối).
- Nếu dataset kết thúc khi vẫn DOWN, event đóng tại timestamp cuối dataset.

### 12.3 Công thức MTTR / MTTF / MTBF
Giả sử có **N** events (FailureCount = N), và mỗi event k có downtime **D_k** (giây):

- **TotalDownTime** = Σ(k=1→N) **D_k**
- Khi **N** > 0:
  - **MTTR** = **TotalDownTime** / **N**
  - **MTTF** = (t₁ + t₂ + … + t_N) / **N**
  - **MTBF** = **MTTF** + **MTTR**

Trong đó (theo paper Duer et al., 2023 và code hiện tại):
- **t₁** = UP time từ **dataset start** đến **failure đầu tiên**
- **t₂** = UP time từ **sau failure 1** đến **failure 2**
- ...
- **t_N** = UP time từ **sau failure (N−1)** đến **failure N**

**Quan trọng**: code **KHÔNG** tính UP time **sau failure cuối cùng** vào MTTF (đúng theo định nghĩa “time to failure”).

**Edge case**:
- Nếu không có failure: `Mttr/Mttf/Mtbf = None`, `FailureCount = 0`

---

## 12.4 Failure charts (UI) — Persist events + tách 2 API

Để UI không phải query lại `ClassificationPoint` và không phải recompute, failure được persist vào DB trong lúc computation:

- **Histogram (Mean number of failure)**:
  - Lấy từ `IndicatorData` (persisted): `failure_count`, `mttr`, `mttf`, `mtbf`
  - API trả unit days (DB vẫn lưu seconds)

- **Timeline (Turbine Failure Chart)**:
  - Lấy từ `FailureEvent` (persisted): danh sách downtime intervals

2 API chart (đều hỗ trợ filter theo thời gian — nếu không truyền thì lấy latest):
- `GET /api/farms/{farm_id}/failure-indicators/?start_time={ms}&end_time={ms}`
- `GET /api/farms/{farm_id}/failure-timeline/?start_time={ms}&end_time={ms}`

FailureEvent model (DB):
- FK: `FailureEvent.computation` (classification computation)
- fields: `start_time_ms`, `end_time_ms`, `duration_s`, `status`

## 13) Weibull fit (module `weibull.py`)

Fit phân phối Weibull cho `WIND_SPEED` (trên tập NORMAL) bằng:
`scipy.stats.weibull_min.fit(wind)` → trả:
- `shape` (k)
- `scale` (λ)

Hàm CDF Weibull:  
**F(v)** = 1 − exp(−(v/λ)^k)

---

## 14) AEP theo Rayleigh/Weibull (module `rayleighs.py`)

**Nguồn:** IEC 61400-12-1:2022, Clause 9.3. AEP tính bằng cách áp dụng measured power curve lên phân phối tần suất tốc độ gió tham chiếu. IEC dùng Rayleigh làm phân phối tham chiếu (Weibull với shape factor 2); AEP tính cho các giá trị annual average wind speed 4, 5, …, 11 m/s (IEC Eq. 17, 18). Có thể thay Rayleigh bằng Weibull với shape/scale từ site (IEC Eq. 19).

Input:
- Power curve “global” theo bin: **P(v_i)**
- Weibull params: (shape **k**, scale **λ**) từ fit trên tập NORMAL.

### 14.1 Rayleigh CDF theo mean wind speed (IEC Eq. 18)
- **F(V)** = 1 − exp(−(π/4)(**V**/**V_ave**)²)

với **V_ave** = annual average wind speed tại hub height. Đây là Rayleigh (Weibull k=2); IEC quy định dùng phân phối này làm tham chiếu.

### 14.2 Tích phân AEP theo trapz trên bins (IEC Eq. 17)
**N_h** = 8760 h/năm. Khởi tạo: **V_{i−1}** = **V_i** − 0,5 m/s, **P_{i−1}** = 0 kW cho bin đầu.

- **AEP** = **N_h** · Σ_{i=1..N} [ **F(V_i)** − **F(V_{i−1})** ] · (**P_{i−1}** + **P_i**)/2  

Code xuất:
- `AepRayleighMeasured{ave}` với `ave` từ 4..11 (tương ứng \(V_{ave}\) 4–11 m/s).

### 14.3 Extrapolation tới V_cutout (IEC 9.3 AEP-extrapolated)
Nếu power curve không phủ tới cut-out, IEC quy định: zero power dưới range đo, **constant power** từ wind speed cao nhất đo được đến cut-out (bằng giá trị bin cao nhất). Code:
- Tạo bins tới `V_cutout`
- Pad/ffill power để không NaN
- Tính lại AEP → `AepRayleighExtrapolated{ave}`

### 14.4 AEP theo Weibull turbine (IEC Eq. 19)
Thay **F** bằng CDF Weibull: **F_W(V)** = 1 − exp(−(**V**/**A_w**)^**k**), với **A_w** = scale (λ trong code), **k** = shape. Output: `AepWeibullTurbine`.

---

## 15) Capacity factor (module `capacity_factor.py`) — theo implementation

Trên data đã bin, với **A** = Swept_area:
- **CapacityFactor(bin)** = **P̄_bin** / (0,6125 · **A** · **V̄_bin**)

Trong đó:
- **P̄_bin** = mean(ACTIVE_POWER) trong bin
- **V̄_bin** = mean(WIND_SPEED) trong bin
- Hằng số **0,6125** = 0,5 · 1,225

Lưu ý: đây là **đúng theo code hiện tại** (không phải công thức chuẩn “power in wind” 0,5·**ρ**·**A**·**V**³).

---

## 16) Output object của `get_wpa`

Theo `smartWPA.process()` trả dict:
- `power_curves`: `{global, yearly, quarterly, monthly, day/night}`
- `weibull`: `{scale, shape}`
- `indicators`: dict KPI (bao gồm `Mttr/Mttf/Mtbf/FailureCount`)
- `CapacityFactor`: dict theo bin
- `classification`: object từ `classification_to_obj` (map + rates + points)
- `start_time`, `end_time`: epoch **seconds** từ index (core compute). Khi lưu DB, API convert sang **milliseconds** nếu cần.

---

## 17) Chuẩn timestamp units (seconds vs milliseconds vs nanoseconds)

Trong hệ thống có 3 dạng timestamp:

- **pandas datetime index**: nanoseconds (nội bộ pandas)
- **epoch seconds**: output của `smartWPA.start_time/end_time`
- **epoch milliseconds**: chuẩn API & DB cho `Computation.start_time/end_time` và `ClassificationPoint.timestamp`

Quy ước:
- API nhận/trả **milliseconds**
- `save_computation_results()` sẽ convert nếu `get_wpa()` trả seconds (nhỏ hơn 1e12)

Checklist khi thêm API mới:
- Tránh mix `ms` và `s` trong cùng payload
- Nếu dùng pandas datetime → convert về ms bằng `astype('int64') // 1e6`

---

## Phụ lục A — Phân loại & gán nhãn dữ liệu (Classification) (gộp từ docs cũ)

Phần này mô tả **cách SmartWPA phân loại trạng thái vận hành** từ SCADA (không dùng status code của turbine) và **vì sao** các rule/thuật toán đó được dùng.

Nguồn code chính:
- `analytics/computation/classifier.py`
- `analytics/computation/normalize.py` (preprocess + làm sạch)

### A.1 Input tối thiểu & giả định

#### A.1.1 Cột bắt buộc
- `TIMESTAMP`
- `WIND_SPEED` (m/s)
- `ACTIVE_POWER` (đơn vị công suất theo SCADA, thường kW)

#### A.1.2 Sampling
Pipeline core giả định dữ liệu chuẩn hoá về **10 phút**:
- Dữ liệu “finer” hơn 10 phút: resample về `10min` (mean)
- Dữ liệu “coarser” hơn 10 phút: báo lỗi (resolution quá thấp)

Lý do: thuật toán banding/rolling-window/merge-event được thiết kế cho time step gần cố định.

---

### A.2 Tập nhãn (status labels)

Backend phân loại mỗi sample vào một trong các nhãn:
- `NORMAL`
- `OVERPRODUCTION`
- `UNDERPRODUCTION`
- `STOP`
- `PARTIAL_STOP`
- `CURTAILMENT`
- `PARTIAL_CURTAILMENT`
- `MEASUREMENT_ERROR`
- `UNKNOWN`

Ý nghĩa:
- **NORMAL**: hoạt động bình thường quanh đường cong khoẻ (healthy curve).
- **OVER/UNDER**: lệch đáng kể so với curve khoẻ (dư/thiếu công suất).
- **STOP**: công suất bằng/nhỏ hơn 0 (theo rule).
- **CURTAILMENT**: “under” nhưng power ổn định bất thường (thường do limit setpoint).
- **PARTIAL_STOP / PARTIAL_CURTAILMENT**: vùng trước/sau stop/curtailment, nghi là bị ảnh hưởng.
- **MEASUREMENT_ERROR**: dữ liệu ngoài ngưỡng hợp lý hoặc bị lỗi cảm biến.

---

### A.3 Quy trình gán nhãn (theo code hiện tại)

#### A.3.1 Bước A — lọc lỗi đo (measurement error)

Mục tiêu: loại bỏ các điểm chắc chắn sai/không dùng được để fit curve khoẻ.

Rule chính (tóm tắt):
- `ACTIVE_POWER <= 0` → `STOP`
- `WIND_SPEED < 0` → `MEASUREMENT_ERROR`
- Ngoài range hợp lý (wind speed/power) → `MEASUREMENT_ERROR`
- Nếu `WIND_SPEED` hoặc `ACTIVE_POWER` là NaN → `MEASUREMENT_ERROR`
- Nếu |**ΔV**| > 10 m/s so với sample trước: mark error và set `WIND_SPEED`, `ACTIVE_POWER` = NaN
- Nếu wind speed “đứng yên” trong tối thiểu 1 giờ → mark error

Vì sao:
- Các điểm sai làm “kéo cong” đường cong khoẻ, dẫn đến under/over bị phân loại sai hàng loạt.

#### A.3.2 Bước B — lọc outlier (DBSCAN)

Trên subset `UNKNOWN` (điểm chưa gán nhãn rõ), chạy DBSCAN trên feature:
- `WIND_SPEED`
- `ACTIVE_POWER`

Mục tiêu:
- Lấy cụm inlier đại diện cho hành vi “khoẻ” để fit curve.

Vì sao:
- Power curve thực tế có thể có nhiễu/nhánh phụ; DBSCAN giúp bỏ các cụm lẻ (outlier) khi chưa có curve chuẩn.

#### A.3.3 Bước C — regression power curve thô (bin + median)

Chia bin theo `bin_width = 0.25` m/s và lấy median power theo bin để tạo “sườn” curve thô.

Vì sao:
- Median robust trước outlier so với mean.
- Binning giảm nhiễu và tạo dạng curve mượt hơn để nội suy.

#### A.3.4 Bước D — healthy band & phân lớp performance

Nội suy spline:
- **P_theoretical(V)** = CubicSpline(center, median)

Chọn band dưới/trên bằng cách quét threshold “độ lệch” (step 10 đến max 1000) và dừng khi tốc độ tăng điểm mới trong band < `stop_threshold`.

Cuối cùng:
- `ACTIVE_POWER < lower_band` → `UNDERPRODUCTION`
- `ACTIVE_POWER > upper_band` → `OVERPRODUCTION`
- còn lại → `NORMAL`

Vì sao:
- Band động (adaptive) giúp phù hợp nhiều turbine/site khác nhau.
- Không phụ thuộc manufacturer curve (thường khác site).

#### A.3.5 Bước E — Curtailment

Curtailment candidate:
- status đang là `UNDERPRODUCTION`
- rolling std của `ACTIVE_POWER` (cửa sổ 30 phút) nhỏ hơn ngưỡng (code hiện tại: 100)

Các chuỗi candidate liên tục đủ dài (>= 30 phút) được gán `CURTAILMENT`.

Vì sao:
- Curtailment thường tạo plateau/setpoint (power ít dao động) khác với underproduction do sự cố.

#### A.3.6 Bước F — Partial curtailment

Gán `PARTIAL_CURTAILMENT` cho các đoạn “normal” gần curtailment nếu thoả điều kiện (đủ dài, không lẫn curtailment).

#### A.3.7 Bước G — Partial stop

Tìm các chuỗi `STOP` liên tục đủ dài (code hiện tại dùng 240 phút).  
Sau đó gán `PARTIAL_STOP` cho các đoạn trước/sau gần nhất nếu thoả điều kiện.

---

## Phụ lục B — Đối chiếu IEC 61400-12-1:2022 và đánh giá

Tài liệu tham chiếu: **IEC 61400-12-1:2022** (Edition 3.0) — *Wind energy generation systems – Part 12-1: Power performance measurements of electricity producing wind turbines*. Dưới đây là mapping giữa tiêu chuẩn và implementation SmartWPA, cùng đánh giá mức độ phù hợp.

### B.1 Tổng quan về IEC 61400-12-1:2022

- **Phạm vi:** Đo đạc hiệu năng công suất của turbine điện gió (một turbine, nối lưới); bao gồm small wind (IEC 61400-2) khi nối lưới hoặc battery.
- **Kết quả chính:** Measured power curve (quan hệ wind speed – power) và **AEP** (annual energy production) ước lượng từ power curve + phân phối gió tham chiếu, giả định availability 100%.
- **Dữ liệu:** Thu thập đồng bộ 10 phút (mean, std, min, max); loại bỏ theo Clause 8.4 (Data rejection); chuẩn hóa theo Clause 9.1 (air density, wind shear, wind veer, turbulence nếu có).

### B.2 Mapping công thức và quy trình

| Hạng mục | IEC 61400-12-1:2022 | SmartWPA (code) | Đánh giá |
|----------|---------------------|------------------|----------|
| **Air density** | Eq. (12): **ρ** từ **T**, **B**, **Φ**; **R₀**=287,05, **R_w**=461,5; **P_w** = 0,0000205·exp(0,0631846·**T**) | `density.py`: cùng **R_air**, **R_vapor**; phần hơi nước dùng **h·0,0631846·T** thay cho **Φ·P_w** | **Gần đúng:** Công thức khí ẩm tương đương; dạng hơi nước đơn giản hóa so với IEC (IEC dùng exp). Nên ghi chú trong tài liệu và cân nhắc dùng P_w theo IEC nếu cần đồng bộ chặt. |
| **Chuẩn hóa theo ρ** | Eq. (13) P_n = P·(ρ₀/ρ); Eq. (14) V_n = V·(ρ/ρ₀)^(1/3) | `normalize.py`: V_n = V·(ρ/1,225)^(1/3), P_n = P·(1,225/ρ) | **Đúng:** Khớp IEC 9.1.5 cho active power control (normalize wind speed) và tỉ lệ power. |
| **Method of bins** | 8.5, 9.2: bin 0,5 m/s, tâm bội số 0,5; mean V và P theo bin | `bins.py` + `curve_est.py`: bin 0,5 m/s, edges 0,25–0,75–…; mean theo bin | **Đúng:** Khớp IEC. |
| **Power curve** | Eqs (15), (16): V_i, P_i = mean trong bin i | Global curve = mean(P) theo bin trên data NORMAL | **Đúng:** Cùng phương pháp. IEC không bắt buộc lọc “chỉ NORMAL”; SmartWPA dùng NORMAL để curve đại diện cho vận hành “khỏe”. |
| **AEP – Rayleigh** | Eq. (17), (18): F(V)=1−exp(−π/4·(V/V_ave)²); AEP cho V_ave = 4..11 m/s | `rayleighs.py`: cùng F_R; AepRayleighMeasured/Extrapolated 4..11 | **Đúng:** Công thức và dải V_ave khớp IEC. |
| **AEP – Weibull** | Eq. (19): F(V)=1−exp(−(V/A_w)^k) | Cùng công thức; scale = λ, shape = k | **Đúng.** |
| **AEP extrapolation** | Zero dưới range; constant power từ bin cao nhất đến cut-out | Pad/ffill tới V_cutout, sau đó trapz | **Phù hợp:** Cách làm tương đương “constant power” từ bin cao nhất. |
| **Cut-in / cut-out** | 3.5, 3.6: định nghĩa; 8.4: loại dữ liệu cut-out hysteresis | Constants: ước lượng từ SCADA (bin hoặc time-series); classifier dùng V_cutin±1, V_cutout+1 để MEASUREMENT_ERROR | **Phù hợp:** Định nghĩa theo IEC; cách ước lượng là “IEC-inspired” (bin 0,5 m/s, ngưỡng 0,05/0,98/0,2 P_rated). |
| **Data rejection (8.4)** | Loại: điều kiện ngoài range, fault, manual shutdown, test/maintenance, lỗi đo, wind direction ngoài sector, v.v. | `filter_error`: power/wind ngoài range, NaN, V ngoài [0,32], P ngoài [-0,05·P_rated, 1,1·P_rated], V ngoài [V_cutin−1, V_cutout+1] khi P>0, |ΔV|>10 m/s, wind “đứng yên” ≥1 h | **Mở rộng:** SmartWPA không dùng status turbine mà suy rejection từ SCADA; 32 m/s phù hợp IEC/kinh nghiệm; các ngưỡng 0,05/1,1 P_rated hợp lý. |
| **Database hoàn chỉnh (8.5)** | Mỗi bin ≥ 30 min; tổng ≥ 180 h | `verify_min_hours`: 180 h; `verify_bin_data_amount`: ít nhất 3 điểm/bin (yêu cầu khác IEC) | **Khác biệt:** IEC 30 min/bin; code 3 điểm/bin (với 10 min ⇒ 30 min nếu đủ 3 điểm). Có thể tăng lên 30 min/bin nếu muốn bám sát IEC. |
| **Availability** | AEP giả định 100 % availability | TBA/PBA và loss theo trạng thái tính riêng; AEP không nhân availability | **Đúng:** AEP theo IEC là “ước lượng năng lượng” với 100 % availability; availability thực tế thể hiện qua TBA/PBA và loss. |

### B.3 Các điểm chưa áp dụng / khác biệt

1. **Wind shear / REWS (9.1.3):** IEC có rotor equivalent wind speed (REWS) và wind shear correction. SmartWPA hiện chỉ dùng hub height wind speed → đúng với “option 4” trong IEC Table 1 (met mast at hub height, chỉ air density normalization); nếu triển khai REWS sẽ giảm uncertainty cho turbine lớn.
2. **Wind veer (9.1.4), turbulence normalization (9.1.6, Annex M):** Chưa implement; IEC khuyến nghị khi cần so sánh power curve giữa site/điều kiện.
3. **Uncertainty (Annex D, E):** IEC yêu cầu báo cáo uncertainty của power curve và AEP. SmartWPA chưa tính/ghi uncertainty theo từng nguồn IEC.
4. **Air density – vapour pressure:** Dùng dạng đơn giản **h·0,0631846·T** thay vì **Φ·P_w** với **P_w** = 0,0000205·exp(0,0631846·**T**); có thể chỉnh lại nếu cần khớp chặt IEC.

### B.4 Kết luận

- **Công thức cốt lõi** (air density normalization, bin 0,5 m/s, power curve mean theo bin, AEP Rayleigh/Weibull, extrapolation) **phù hợp hoặc khớp** IEC 61400-12-1:2022.
- **Classification trạng thái** là mở rộng so với IEC (IEC chỉ rejection/accept); việc dùng nhiều nhãn (NORMAL, STOP, CURTAILMENT, …) phục vụ WPA và reliability (IEC TS 61400-26) là hợp lý.
- **Nên bổ sung:** (1) Ghi rõ trong tài liệu sự khác biệt công thức hơi nước trong air density; (2) Cân nhắc điều kiện “database hoàn chỉnh” theo IEC (30 min/bin); (3) Khi có nhu cầu báo cáo chuẩn, bổ sung uncertainty theo Annex D/E và/hoặc REWS nếu có dữ liệu đo nhiều tầng cao.


