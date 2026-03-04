# KPI dictionary — Công thức, đơn vị, nguồn code, và “vì sao dùng”

Tài liệu này là “dictionary” cho toàn bộ các chỉ số (KPI) SmartWPA đang tính và/hoặc trả qua API.

Mục tiêu:

- Mapping **KPI → công thức → nguồn code → đơn vị → ý nghĩa/vì sao dùng**
- Chỉ ra **mismatch** giữa model/docs và code (đặc biệt về **đơn vị**)

Nguồn code chính:

- Core compute: `analytics/computation/*`
- Persist/output: `api_gateway/turbines_analysis/helpers/computation_helper.py`, `analytics/models.py`
- Raw SCADA schema: `acquisition/models.py`

---

## 0) Quy ước đơn vị & ký hiệu

- Sampling (giả định): 10 phút (time step \(\Delta t\))
- \(P_i\): `ACTIVE_POWER` tại sample i
- \(\hat{P}_i\): `ESTIMATED_POWER` tại sample i
- \(V_i\): `WIND_SPEED` tại sample i
- \(\Delta t_h\): time step theo giờ \(\Delta t_h = \Delta t / 1h\)

**Lưu ý quan trọng về năng lượng**:

\[
E \approx \sum P \cdot \Delta t_h
\]

=> đơn vị của \(E\) phụ thuộc đơn vị \(P\):

- Nếu `ACTIVE_POWER` là **kW** → \(E\) là **kWh**
- Nếu `ACTIVE_POWER` là **MW** → \(E\) là **MWh**

Hiện tại code **không convert** kW/MW, nên tính nhất quán phụ thuộc dữ liệu đầu vào.

---

## 1) Nhóm KPI “Energy & Production” (file `analytics/computation/indicators.py`)

Nguồn: `analytics/computation/indicators.py` + `analytics/computation/estimate.py`

### 1.1 AverageWindSpeed
- **Công thức**: \(\text{mean}(V_i)\)
- **Code**: `estimated_data['WIND_SPEED'].mean()`
- **Đơn vị**: m/s
- **Vì sao dùng**: baseline điều kiện gió trong time range (phục vụ so sánh turbine/time bucket).

### 1.2 ReachableEnergy
- **Công thức**: \(\sum \hat{P}_i \cdot \Delta t_h\)
- **Code**: `ESTIMATED_POWER.sum() * (resolution / 1h)`
- **Đơn vị**: phụ thuộc `ACTIVE_POWER` (kWh/MWh)
- **Vì sao dùng**: “sản lượng kỳ vọng” nếu không có losses, dùng làm mẫu số cho LossPercent và PBA.

### 1.3 RealEnergy
- **Công thức**: \(\sum P_i \cdot \Delta t_h\)
- **Đơn vị**: phụ thuộc `ACTIVE_POWER` (kWh/MWh)
- **Vì sao dùng**: sản lượng thực tế.

### 1.x Mapping tên KPI → field DB (`analytics.IndicatorData`)

Khi persist vào DB, các key từ `indicators()` được map sang `analytics.models.IndicatorData` (snake_case). Ví dụ:

- `AverageWindSpeed` → `average_wind_speed`
- `ReachableEnergy` → `reachable_energy`
- `RealEnergy` → `real_energy`
- `LossEnergy` → `loss_energy`
- `LossPercent` → `loss_percent`
- `RatedPower` → `rated_power`
- `Tba` → `tba`
- `Pba` → `pba`
- `FailureCount` → `failure_count`
- `Mttr/Mttf/Mtbf` → `mttr/mttf/mtbf` (**seconds**)

Lưu ý: DB hiện có thêm nhiều field AEP Rayleigh/Weibull, losses, durations… nên KPI dictionary này nên được dùng làm nguồn “single source of truth” cho naming/units.

### 1.4 LossEnergy
- **Công thức**: \(\max(0, ReachableEnergy - RealEnergy)\)
- **Vì sao dùng**: lượng mất mát có thể quy về production loss.

### 1.5 LossPercent
- **Công thức**:

\[
LossPercent =
\begin{cases}
\frac{LossEnergy}{ReachableEnergy}, & ReachableEnergy > 0 \\
0, & \text{ngược lại}
\end{cases}
\]

- **Vì sao dùng**: tỷ lệ mất mát chuẩn hoá theo điều kiện gió/time range.

### 1.6 DailyProduction (output dạng list theo ngày)
- **Công thức (Real)**: group theo ngày: \(E_{day} = \sum_{i\in day} P_i \cdot \Delta t_h\)
- **Công thức (Reachable)**: group theo ngày: \(E_{day,reach} = \sum_{i\in day} \hat{P}_i \cdot \Delta t_h\) với \(\hat{P}_i\) = ESTIMATED_POWER
- **Đơn vị**: phụ thuộc `ACTIVE_POWER` (thường kWh)
- **DB model**: `DailyProduction` lưu `daily_production` (real) và `daily_reachable` (reachable, nullable cho data cũ)
- **Vì sao dùng**: phục vụ dashboard theo thời gian — biểu đồ PRODUCTION cần cả 3 series: Reachable, Real, Loss (= Reachable - Real).
- **Monthly Dashboard API**: Tổng hợp theo tháng trả `{"month_start_ms", "production", "reachable", "loss"}`. Nếu chưa chạy lại computation thì `reachable` và `loss` = `null`.

---

## 2) Availability KPIs (TBA/PBA)

Nguồn: `analytics/computation/indicators.py`

### 2.1 Tba (Time based availability)
- **Công thức**: \(Tba = \frac{R}{R+U}\)
- **Code**: `R` là count các sample thuộc `NORMAL/CURTAILMENT/PARTIAL_CURTAILMENT/OVERPRODUCTION/UNDERPRODUCTION`, `U` là count `STOP/PARTIAL_STOP`.
- **Đơn vị**: ratio (0..1)
- **Vì sao dùng**: availability theo “thời gian”, phù hợp khi sampling đều.

### 2.2 Pba (Production based availability)
- **Công thức**: \(Pba = \frac{\sum P}{\sum \hat{P}}\) trên subset `status != MEASUREMENT_ERROR`
- **Đơn vị**: ratio
- **Vì sao dùng**: availability theo “sản lượng”, phản ánh underperformance/curtailment.

---

## 3) Loss theo trạng thái vận hành

Nguồn: `analytics/computation/indicators.py`

Các KPI:
- `StopLoss`
- `PartialStopLoss`
- `UnderProductionLoss`
- `CurtailmentLoss`
- `PartialCurtailmentLoss`

**Công thức chung**:

\[
Loss_S = \max\left(0, \sum_{i\in S}(\hat{P}_i - P_i)\cdot \Delta t_h\right)
\]

**Vì sao dùng**:
- Quy đổi từng trạng thái vận hành thành “mất năng lượng” để Pareto/diagnosis.

---

## 4) “Counts & Durations” (time step, duration, point counts)

Nguồn: `analytics/computation/indicators.py`

- `TotalStopPoints`, `TotalPartialStopPoints`, `TotalUnderProductionPoints`, `TotalCurtailmentPoints`: count theo label
- `TimeStep`: `resolution.total_seconds()` (seconds)
- `TotalDuration`: `(max_ts - min_ts).total_seconds()` (seconds)
- `DurationWithoutError`: `TotalDuration - TimeStep * (#STOP + #PARTIAL_STOP)`

**Vì sao dùng**:
- Phục vụ sanity-check dữ liệu và các KPI “time-based”.

---

## 5) Yaw error KPIs

Nguồn: `analytics/computation/yaw_error.py`

### 5.1 YawLag.data (histogram)
- **Định nghĩa**: \(\Delta = \theta_{nacelle} - \theta_{wind}\), normalize về \([-180,180)\)
- Histogram theo bin 10° (mặc định).

### 5.2 YawLag.statistics
- `mean_error`, `median_error`, `std_error`
- **Đơn vị**: degrees

**Vì sao dùng**:
- Yaw misalignment là nguyên nhân quan trọng gây loss/underperformance.

---

## 6) Reliability / Failure Analysis (MTTR/MTTF/MTBF)

Nguồn: `analytics/computation/reliability.py`

Mapping strict:
- UP: `NORMAL`, `OVERPRODUCTION`
- DOWN: `STOP`
- OTHER ignored: `PARTIAL_STOP`, `CURTAILMENT`, `PARTIAL_CURTAILMENT`, `UNDERPRODUCTION`, `MEASUREMENT_ERROR`, `UNKNOWN`

### 6.1 FailureCount
Số failure events = số transition **UP→DOWN** (merge DOWN liên tiếp).

### 6.2 MTTR
\[
MTTR = \frac{TotalDownTime}{FailureCount}
\]

### 6.3 MTTF (strict)
\[
MTTF = \frac{t_1 + t_2 + ... + t_N}{N}
\]

Trong đó:
- \(t_1\): UP time từ dataset start đến failure đầu tiên
- \(t_i\): UP time giữa failure (i−1) và i
- **Không** tính UP time sau failure cuối.

### 6.4 MTBF
\[
MTBF = MTTF + MTTR
\]

**Đơn vị**:
- Core compute: seconds
- API histogram: days (seconds / 86400)

**Vì sao dùng**:
- KPI reliability chuẩn hoá, phù hợp cho dashboard farm/turbine và trend theo thời gian.

---

### 6.5 Reliability mapping (strict) — quy tắc “OTHER không ảnh hưởng transition”

Trong SmartWPA, reliability KPI được tính theo “strict mode” (đang implement trong `analytics/computation/reliability.py` và được gọi từ `analytics/computation/indicators.py`).

- **UP** (fit/operating): `NORMAL`, `OVERPRODUCTION`
- **DOWN** (failure/repair): `STOP`
- **OTHER** (ignored/degraded): `PARTIAL_STOP`, `CURTAILMENT`, `PARTIAL_CURTAILMENT`, `UNDERPRODUCTION`, `MEASUREMENT_ERROR`, `UNKNOWN`

Nguyên tắc “strict” khi duyệt time-series:

- `OTHER` **không** được tính là UP hay DOWN.
- `OTHER` **không** được phép mở/đóng failure event.
- `OTHER` **không** làm thay đổi `last_meaningful_state` khi xét transition.  
  (Tức là khi gặp `OTHER`, bộ nhớ trạng thái trước đó vẫn giữ nguyên để không tạo false transitions.)

Tác dụng:
- Tránh việc curtailment/partial_stop làm “gãy” chuỗi UP/DOWN và tạo FailureCount sai.

---

### 6.6 FailureEvent (timeline) — định nghĩa, thuật toán derive, và edge-cases

**FailureEvent** là interval downtime dùng cho chart timeline, được define:

> **FailureEvent = transition UP → STOP**, và các sample STOP liên tiếp được merge thành **1 interval**.

Thuật toán (mô tả sát code, giản lược):

- Duyệt status theo thời gian, **bỏ qua** toàn bộ sample thuộc `OTHER`.
- Khi gặp `STOP` và `last_meaningful_state == UP`:
  - mở event với `start_time = ts_current`
  - `in_down = True`
- Khi đang `in_down` và gặp lại `UP`:
  - đóng event tại **timestamp DOWN cuối**:
    - `end_time = ts_up - dt` (dt là time step)
  - lưu `duration_s = (end_time - start_time)/1000` (hoặc theo seconds trong compute)
  - `in_down = False`
- Nếu hết dataset mà vẫn `in_down`:
  - đóng event tại timestamp cuối dataset.

Edge cases:
- Nếu không có transition UP→STOP: `FailureCount = 0`, không có FailureEvent.
- Nếu dataset bắt đầu bằng STOP nhưng chưa có “UP trước đó”:
  - strict mode **không** tính đây là failure transition (tuỳ cách khởi tạo last_state).
  - Điều này giúp tránh “đếm lỗi” khi data window bắt đầu giữa downtime.

---

### 6.7 Persist nguồn dữ liệu reliability (DB) — 1 nguồn sự thật cho UI

#### 6.7.1 Histogram (Reliability indicators)

Persist trong `analytics.IndicatorData` (đơn vị **seconds**):
- `failure_count`
- `mttr`, `mttf`, `mtbf`

API chart trả về:
- `Mttr/Mttf/Mtbf` theo **days** (seconds / 86400) để UI hiển thị thống nhất.

#### 6.7.2 Timeline (Failure events)

Persist trong `analytics.FailureEvent`:
- `start_time` (ms)
- `end_time` (ms)
- `duration_s` (seconds)
- `status` (default `"STOP"`)

---

### 6.8 Failure Chart APIs — endpoints, units, và schema payload

SmartWPA có 2 API failure chart:

- **Histogram (indicators)**: `GET /api/farms/{farm_id}/failure-indicators/`
- **Timeline (Gantt)**: `GET /api/farms/{farm_id}/failure-timeline/`

Query params (optional): `start_time` (ms), `end_time` (ms).  
Nếu không truyền, API dùng computation latest theo turbine.

**Unit conventions**:
- `Mttr/Mttf/Mtbf`: **days**
- `duration_s`: **seconds**
- mọi timestamp `start_time`/`end_time`: **milliseconds**

---

### 6.9 Schema chi tiết — Failure indicators chart API

Response (farm):

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1640995200000,
    "end_time": 1672531200000,
    "turbines": [
      {
        "turbine_id": 1,
        "turbine_name": "WT1",
        "summary": {
          "FailureCount": 5,
          "Mttr": 2.0,
          "Mttf": 30.0,
          "Mtbf": 32.0
        }
      }
    ]
  }
}
```

Ghi chú:
- `Mttr/Mttf/Mtbf` trong payload chart là **days**.
- `FailureCount` là integer.

---

### 6.10 Schema chi tiết — Failure timeline chart API

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1640995200000,
    "end_time": 1672531200000,
    "months": [1640995200000, 1643673600000],
    "turbines": [
      {
        "turbine_id": 1,
        "turbine_name": "WT1",
        "events": [
          {
            "start_time": 1640995200000,
            "end_time": 1641002400000,
            "duration_s": 7200.0,
            "status": "STOP"
          }
        ]
      }
    ]
  }
}
```

Ghi chú:
- `duration_s` vẫn giữ **seconds** (không convert) để UI hiển thị chính xác.
- `months` là mảng month-start timestamps (ms) để FE vẽ trục X (J, F, M, ...).
- FE vẽ nền xanh (operating) cho toàn bộ trục, sau đó đè cam cho mỗi event.

---

## 7) Weibull + AEP (Rayleigh/Weibull)

Nguồn:
- Weibull fit: `analytics/computation/weibull.py`
- AEP: `analytics/computation/rayleighs.py`

### 7.1 Weibull parameters
- Fit `weibull_min.fit(wind)` → `shape (k)`, `scale (λ)`

### 7.2 Rayleigh AEP (Measured / Extrapolated)
Code tính theo bins và xấp xỉ tích phân bằng trapezoid:

\[
AEP \approx 8760 \cdot \sum_i \Delta F_i \cdot \frac{P(v_{i-1})+P(v_i)}{2}
\]

### 7.3 Weibull turbine AEP
Thay Rayleigh CDF bằng Weibull CDF:

\[
F_W(v)=1-\exp\left(-\left(\frac{v}{\lambda}\right)^k\right)
\]

**Đơn vị**: phụ thuộc unit power curve (kW/MW) → AEP tương ứng (kWh/MWh)/year

**Vì sao dùng**:
- Ước lượng sản lượng năm theo phân phối gió, phục vụ benchmark.

---

## 8) Air density & normalize theo \(\rho_0=1.225\)

Nguồn:
- `analytics/computation/density.py`
- `analytics/computation/normalize.py`
- **Tiêu chuẩn:** IEC 61400-12-1:2022, Clause 9.1.5 (Equations 12, 13, 14). Chi tiết mapping và đánh giá: `docs/QUY_TRINH_TINH_TOAN_COMPUTATION.md`, Phụ lục B.

### 8.1 Air density
\[
\rho = \frac{1}{T}\left(\frac{p}{R_{air}} - h \cdot 0.0631846 \cdot T \cdot \left(\frac{1}{R_{air}} - \frac{1}{R_{vapor}}\right)\right)
\]
- \(R_{air}=287{,}05\) J/(kg·K), \(R_{vapor}=461{,}5\) J/(kg·K) (IEC \(R_0\), \(R_w\)). IEC 9.1.5 dùng áp suất hơi nước \(P_w = 0{,}0000205 \exp(0{,}0631846 \cdot T)\) [Pa]; code dùng dạng tuyến tính theo \(h \cdot T\) — gần đúng, xem Phụ lục B.

### 8.2 Normalize wind speed & power (IEC Eqs 13, 14)
\[
V_{norm} = V \cdot \sqrt[3]{\frac{\rho}{1.225}}
\]
\[
P_{norm} = P \cdot \frac{1.225}{\rho}
\]

**Vì sao dùng**:
- Chuẩn hoá về điều kiện khí quyển chuẩn giúp power curve ổn định hơn; công thức tương ứng IEC cho turbine điều khiển công suất chủ động.

---

## 9) CapacityFactor — cảnh báo mismatch (rất quan trọng)

Nguồn: `analytics/computation/capacity_factor.py`

**Code hiện tại**:

\[
CapacityFactor(bin)=\frac{\overline{P}_{bin}}{0.6125 \cdot A \cdot \overline{V}_{bin}}
\]

Trong đó \(A = Swept\_area\), \(0.6125 = 0.5 \cdot 1.225\).

### 9.1 Vấn đề
- Công thức này **không phải** “capacity factor” chuẩn ngành (chuẩn thường là \(E_{real}/(P_{rated}\cdot T)\)).
- Mẫu số cũng không phải “power in wind” chuẩn \(0.5\rho A V^3\) (thiếu \(V^2\)).

### 9.2 Khuyến nghị
- **Nếu bạn muốn capacity factor chuẩn**:
  - Định nghĩa:

\[
CF = \frac{RealEnergy}{P_{rated} \cdot T}
\]

  - Trong đó \(T\) là tổng thời gian (hours) trong range.
  - Khi đó nên persist `CapacityFactorOverall` vào `IndicatorData` (hoặc bảng riêng) và/hoặc trả qua API dashboard.
- **Nếu muốn giữ chỉ số hiện tại**: nên đổi tên để tránh hiểu nhầm, ví dụ `AerodynamicScalingIndex` và mô tả rõ.

---

## 10) Danh sách mismatch quan trọng (cần xử lý/chuẩn hoá)

### 10.1 Mismatch đơn vị trong raw SCADA model (`acquisition.FactoryHistorical`)

Trong `acquisition/models.py`, metadata hiện mô tả:
- `active_power`: “MW”
- `air_temp`: “°C”
- `pressure`: “%”
- `hud`: “%”

```70:83:D:\SmartWPA\acquisition\models.py
class FactoryHistorical(models.Model):
    ...
    active_power = models.FloatField(null=True, verbose_name='Active Power (MW)')
    ...
    air_temp = models.FloatField(null=True, verbose_name='Ambient Temperature (oC)')
    pressure = models.FloatField(null=True, verbose_name='Air pressure (%)')
    hud = models.FloatField(null=True, verbose_name='Relative humidity (%)')
```

Nhưng core compute đang kỳ vọng (theo logic/ngưỡng xử lý):
- `PRESSURE` là **Pa** (lọc outlier 50k..108.5k trong `normalize.py`)
- `HUMIDITY` normalize về **0..1** (nếu >1 thì /100)
- `TEMPERATURE` chuyển sang **Kelvin** nếu mean < 223

=> Cần thống nhất:
- Hoặc sửa ingestion mapping để `pressure` vào compute là Pa (nếu raw đang % thì phải đổi công thức/nguỡng)
- Hoặc sửa compute để xử lý pressure theo % nếu đúng là % (hiện tại sẽ mark error/impute sai).

#### 10.1.1 Trạng thái hiện tại sau chuẩn hoá end-to-end

Backend đã bổ sung cấu hình units theo farm/turbine tại `acquisition.ScadaUnitConfig` và normalize về canonical units ngay khi load SCADA (DB/file) trước khi chạy compute.

- Canonical trong compute:
  - `ACTIVE_POWER`: kW
  - `PRESSURE`: Pa (nếu pressure config là `percent/unknown` thì drop cột `PRESSURE` để density fallback)
  - `TEMPERATURE`: K
  - `HUMIDITY`: ratio

Khi gọi API `computation`/`timeseries`/`cross-data-analysis`, response sẽ kèm `units` metadata cho biết canonical + raw_config đang dùng.

### 10.2 CapacityFactor naming mismatch (đã nêu ở mục 9)

### 10.3 Chained assignment bug (ảnh hưởng chất lượng dữ liệu met)

Trong `analytics/computation/normalize.py`, các hàm `remove_humidity_outliers` và `remove_pressure_outliers` dùng pattern `data[mask]['COL'] = np.nan` (chained indexing) có thể **không ghi được vào DataFrame gốc** (pandas SettingWithCopy).

Hậu quả:
- outlier có thể không bị set NaN
- imputation có thể chạy trên dữ liệu vẫn còn outlier

Khuyến nghị:
- sửa thành `data.loc[mask, 'HUMIDITY'] = np.nan` và tương tự cho `PRESSURE`.

---

## 11) Checklist kiểm chứng (sanity tests) cho KPI

- **Energy consistency**: `LossEnergy ≈ ReachableEnergy - RealEnergy` và không âm.
- **Range**: `LossPercent` trong [0,1] nếu ReachableEnergy > 0.
- **Availability**: `Tba` trong [0,1], `Pba` thường trong [0,~1.2] (tuỳ dữ liệu).
- **Reliability**: `FailureCount` = số `FailureEvent` persisted trong cùng range; `Mtbf = Mttr + Mttf`.
- **Units**:
  - Nếu `ACTIVE_POWER` đổi kW/MW, toàn bộ energy & AEP scale theo.
  - `PRESSURE` phải là Pa nếu dùng công thức air density hiện tại.

---

## 12) “Operating constants” (V_cutin/V_cutout/V_rated/P_rated) — công thức & vì sao dùng

Nguồn: `analytics/computation/constants_estimation.py` (IEC-inspired)

Các constants này được persist trong `analytics.Computation` và dùng xuyên suốt:
- filter lỗi đo (cut-in/out margin)
- verify coverage (normal points cover đủ vùng gió/công suất)
- làm input cho nhiều KPI/AEP

### 12.1 P_rated (Rated power)

- **Công thức (robust)**:

\[
P_{rated} \approx median(\text{top }0.5\%\ \text{các điểm } P\ge 0)
\]

- **Vì sao dùng**:
  - giảm nhạy với spike/outlier (median thay vì max)
  - không phụ thuộc nameplate (có thể thiếu trong SCADA)

### 12.2 V_rated (Rated wind speed)

- Binning 0.5 m/s, chọn **bin nhỏ nhất** thoả:

\[
\overline{P}_{bin} \ge 0.98 \cdot P_{rated},\ \ N_{bin}\ge 30
\]

- Fallback: bin có \(\overline{P}\) lớn nhất.

### 12.3 V_cutin (Cut-in wind speed)

- Xét vùng \(V < V_{rated}\), chọn **bin nhỏ nhất** thoả:

\[
\overline{P}_{bin} > 0.05 \cdot P_{rated},\ \ N_{bin}\ge 30
\]

### 12.4 V_cutout (Cut-out wind speed)

- Xét vùng \(V > V_{rated}\), chọn **bin nhỏ nhất** thoả đồng thời:

\[
\overline{P}_{bin} < 0.2 \cdot P_{rated}
\]
\[
\text{ratio}(P < 0.02\cdot P_{rated}) > 0.70
\]

### 12.5 Cảnh báo unit (liên quan trực tiếp)

- `analytics.Computation.p_rated` đang mô tả “kW” (help_text).
- Nếu raw SCADA đang là MW mà không convert → constants + KPI energy/AEP sẽ lệch scale.

---

## 13) Cross Data Analysis — tham số và tính năng API

Nguồn: `api_gateway/turbines_analysis/cross_data_analysis.py` + `helpers/cross_data_analysis_helpers.py`

Tham chiếu manual: Meteodyn WPA User Manual, mục 1.3.6.2.7

### 13.1 Regression types

API hỗ trợ 7 loại hồi quy qua `regression.type`:

| Type | Mô hình | Điều kiện |
|------|---------|-----------|
| `linear` | y = a*x + b | Mặc định. Hỗ trợ `force_zero_intercept` |
| `polynomial2` | y = ax² + bx + c | Cần >= 3 điểm |
| `polynomial3` | y = ax³ + bx² + cx + d | Cần >= 4 điểm |
| `polynomial4` | y = ax⁴ + ... + e | Cần >= 5 điểm |
| `exponential` | y = a * exp(b*x) | Yêu cầu y > 0 |
| `power` | y = a * x^b | Yêu cầu x > 0 và y > 0 |
| `logarithmic` | y = a * ln(x) + b | Yêu cầu x > 0 |

Response regression: `{ type, coefficients, equation, r2, rmse }`

### 13.2 Group by options

| group_by | Mô tả |
|----------|-------|
| `none` | Không gom nhóm |
| `classification` | Tô màu theo trạng thái vận hành (NORMAL, STOP, ...) |
| `monthly` | Gom theo tháng cụ thể: "2023-01", "2023-02" |
| `yearly` | Gom theo năm: "2023", "2024" |
| `seasonally` | Gom theo quý cụ thể: "2023Q1", "2023Q2" |
| `time_profile_monthly` | Gom theo tên tháng bất kể năm: "Jan", "Feb", ... |
| `time_profile_seasonally` | Gom theo quý bất kể năm: "Q1", "Q2", "Q3", "Q4" |
| `source` | Trục Z: tô màu theo giá trị liên tục của 1 source (binning) |

Cross Data Analysis theo manual (1.3.6.2.7) **chỉ có ở turbine level**; không hỗ trợ farm-level hay `group_by: turbine`.

**Response:** Theo manual, API không trả metadata đơn vị (`units`) hay nguồn dữ liệu (`data_source_used`) trong output. Response chỉ gồm: metadata turbine, `x_source`, `y_source`, `group_by`, `regression`, `period`, `summary` (rows_before_filters, rows_after_filters, points_returned), `points`, và tùy chọn `statistics`. Nếu sau này bổ sung `units`/source thì coi là mở rộng.

### 13.3 Classification filter

- `filters.classifications`: danh sách trạng thái cần giữ, vd `["NORMAL", "CURTAILMENT"]`. Bỏ trống = giữ tất cả.
- `only_computation_data`: khi `true`, chỉ dùng dữ liệu từ ClassificationPoint (đã qua computation).

### 13.4 Statistics (optional)

Khi `include_statistics: true`, response bổ sung `statistics`:
- `x_histogram`, `y_histogram`: histogram 30 bins
- `x_stats`, `y_stats`: mean, std, min, max, median, count

Statistics được tính **trước khi downsample** để đảm bảo phân phối chính xác.

### 13.5 Backlog (manual có, API chưa)

- **Regression "Linear variance ratio"**: manual 1.3.6.2.7 liệt kê loại này; API hiện chưa hỗ trợ, có thể bổ sung sau.
- **Group "Time profile Yearly"**: manual có Time profile → Monthly, Seasonally, Yearly; API hiện chỉ có `time_profile_monthly` và `time_profile_seasonally`, chưa có `time_profile_yearly`.
