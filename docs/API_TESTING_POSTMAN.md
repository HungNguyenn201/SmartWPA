# Test toàn bộ API bằng Postman — Hướng dẫn chi tiết

Tài liệu này hướng dẫn test **từng API** của SmartWPA backend bằng Postman: mỗi endpoint đều có mô tả tham số đầu vào, các trường hợp truyền tham số, và output mẫu.

> **Quy ước chung**: Mọi timestamp trong hệ thống đều là **Unix epoch milliseconds** (13 chữ số, ví dụ `1325376000000` = 2012-01-01 00:00:00 UTC).

---

## Mục lục

- [0. Chuẩn bị](#0-chuẩn-bị)
- [1. Auth (Xác thực)](#1-auth-xác-thực)
- [2. CRUD: Farms / Turbines / Acquisition](#2-crud-farms--turbines--acquisition)
- [3. Computation (Chạy tính toán)](#3-computation-chạy-tính-toán)
- [4. Turbine-level Analysis APIs](#4-turbine-level-analysis-apis)
  - [4.1 Classification Rate](#41-classification-rate)
  - [4.2 Indicators (Turbine)](#42-indicators-turbine)
  - [4.3 Power Curve (Line + Scatter)](#43-power-curve-line--scatter)
  - [4.4 Weibull (Turbine)](#44-weibull-turbine)
  - [4.5 Yaw Error](#45-yaw-error)
  - [4.6 Timeseries](#46-timeseries)
  - [4.7 Working Period](#47-working-period)
  - [4.8 Wind Speed Analysis](#48-wind-speed-analysis)
  - [4.9 Distribution](#49-distribution)
  - [4.10 Static Table (Thống kê tĩnh)](#410-static-table)
  - [4.11 Time Profile](#411-time-profile)
  - [4.12 Cross Data Analysis (Turbine)](#412-cross-data-analysis-turbine)
  - [4.13 Monthly Dashboard (Turbine)](#413-monthly-dashboard-turbine)
- [5. Farm-level Analysis APIs](#5-farm-level-analysis-apis)
  - [5.1 Indicators (Farm)](#51-indicators-farm)
  - [5.2 Weibull (Farm)](#52-weibull-farm)
  - [5.3 Power Curve (Farm)](#53-power-curve-farm)
  - [5.4 Failure Indicators (Histogram)](#54-failure-indicators-histogram)
  - [5.5 Failure Timeline (Gantt)](#55-failure-timeline-gantt)
  - [5.6 Monthly Dashboard (Farm)](#56-monthly-dashboard-farm)
- [6. Negative Tests (Kiểm thử lỗi)](#6-negative-tests)
- [7. Postman Collection](#7-postman-collection)

---

## 0. Chuẩn bị

### Response schema thống nhất

Mọi API trả về JSON theo cấu trúc:

**Thành công (2xx):**

```json
{ "success": true, "data": { ... } }
```

**Lỗi (4xx/5xx):**

```json
{ "success": false, "error": "Mô tả lỗi", "code": "ERROR_CODE" }
```

Các `code` lỗi thường gặp: `MISSING_PARAMETERS`, `INVALID_PARAMETERS`, `TURBINE_NOT_FOUND`, `FARM_NOT_FOUND`, `NO_DATA`, `NO_RESULT_FOUND`, `NO_COMPUTATION`, `INVALID_TIME_RANGE`, `INTERNAL_SERVER_ERROR`.

### Base URL

```
http://127.0.0.1:8000
```

### Environment variables (Postman)

| Variable | Mô tả | Ví dụ |
|----------|--------|-------|
| `base_url` | URL server | `http://127.0.0.1:8000` |
| `access_token` | JWT access token (lấy sau login) | `eyJhbGciOi...` |
| `refresh_token` | JWT refresh token | `eyJhbGciOi...` |
| `farm_id` | ID farm đang test | `1` |
| `turbine_id` | ID turbine đang test | `1` |
| `start_time_ms` | Timestamp bắt đầu (ms) | `1325376000000` |
| `end_time_ms` | Timestamp kết thúc (ms) | `1356998400000` |

### Authorization

Tất cả API (trừ login) yêu cầu header:

```
Authorization: Bearer {{access_token}}
```

Trong Postman: tab **Authorization** → Type **Bearer Token** → Token = `{{access_token}}`.

---

## 1. Auth (Xác thực)

### 1.1 Login

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/auth/login/` |
| **Body** | JSON |

**Tham số:**

| Field | Type | Bắt buộc | Mô tả |
|-------|------|----------|-------|
| `username` | string | Có | Tên đăng nhập |
| `password` | string | Có | Mật khẩu |

**Request:**

```json
{
  "username": "admin",
  "password": "password"
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "token": {
      "access": "eyJhbGciOiJIUzI1NiIs...",
      "refresh": "eyJhbGciOiJIUzI1NiIs..."
    },
    "user": {
      "id": 1,
      "username": "admin",
      "role": "admin"
    }
  }
}
```

> **Postman tip**: Trong tab **Tests**, thêm script tự lưu token:
> ```js
> var res = pm.response.json();
> pm.environment.set("access_token", res.data.token.access);
> pm.environment.set("refresh_token", res.data.token.refresh);
> ```

**Test case lỗi:**

```json
// Sai mật khẩu
{ "username": "admin", "password": "wrong" }
// → 401 Unauthorized
```

```json
// Thiếu field
{ "username": "admin" }
// → 400 Bad Request
```

---

### 1.2 Refresh Token

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/auth/refresh/` |
| **Body** | JSON |

**Request:**

```json
{
  "refresh": "{{refresh_token}}"
}
```

**Response (200):**

```json
{
  "access": "eyJhbGciOiJIUzI1NiIs..."
}
```

**Test case lỗi:**

```json
// Token hết hạn hoặc sai
{ "refresh": "invalid_token" }
// → 401 Unauthorized
```

---

### 1.3 Logout

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/auth/logout/` |
| **Body** | JSON |

**Request:**

```json
{
  "refresh_token": "{{refresh_token}}"
}
```

**Response (200):**

```json
{
  "success": true,
  "message": "Logged out successfully"
}
```

---

## 2. CRUD: Farms / Turbines / Acquisition

### 2.1 Farm CRUD

#### List Farms

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/` |

Không cần tham số. Trả danh sách farm mà user có quyền truy cập.

**Response (200):**

```json
{
  "success": true,
  "data": [
    { "id": 1, "name": "Farm A", "address": "VN", "capacity": 100, "latitude": 10.0, "longitude": 106.0 }
  ]
}
```

#### Create Farm

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/farms/create/` |

**Tham số:**

| Field | Type | Bắt buộc | Mô tả |
|-------|------|----------|-------|
| `name` | string | Có | Tên farm |
| `address` | string | Không | Địa chỉ |
| `capacity` | float | Không | Công suất lắp đặt (MW) |
| `latitude` | float | Không | Vĩ độ |
| `longitude` | float | Không | Kinh độ |

**Request:**

```json
{
  "name": "Farm A",
  "address": "VN",
  "capacity": 100,
  "latitude": 10.0,
  "longitude": 106.0
}
```

#### Get Farm Details

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/` |

#### Update Farm

| | |
|---|---|
| **Method** | PUT |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/update/` |

Body: giống Create, chỉ gửi field cần cập nhật.

#### Delete Farm

| | |
|---|---|
| **Method** | DELETE |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/delete/` |

---

### 2.2 Turbine CRUD

#### Create Turbine

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/turbines/create/` |

**Tham số:**

| Field | Type | Bắt buộc | Mô tả |
|-------|------|----------|-------|
| `name` | string | Có | Tên turbine (vd: "WT1") |
| `capacity` | float | Không | Công suất (MW) |
| `latitude` | float | Không | Vĩ độ |
| `longitude` | float | Không | Kinh độ |

**Request:**

```json
{
  "name": "WT1",
  "capacity": 5.0,
  "latitude": 10.0,
  "longitude": 106.0
}
```

#### List Turbines

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/` |

#### Get / Update / Delete Turbine

- **GET** `{{base_url}}/api/turbines/{{turbine_id}}/`
- **PUT** `{{base_url}}/api/turbines/{{turbine_id}}/update/`
- **DELETE** `{{base_url}}/api/turbines/{{turbine_id}}/delete/`

---

### 2.3 Acquisition (SmartHIS / HISPoint)

#### Create SmartHIS

- **POST** `{{base_url}}/api/smart-his/create/`

#### List Point Types

- **GET** `{{base_url}}/api/point-types/`

#### HISPoint CRUD

- **GET** `{{base_url}}/api/his-points/`
- **POST** `{{base_url}}/api/his-points/create/`
- **GET** `{{base_url}}/api/his-points/{{his_point_id}}/`
- **PUT** `{{base_url}}/api/his-points/{{his_point_id}}/update/`
- **DELETE** `{{base_url}}/api/his-points/{{his_point_id}}/delete/`

---

## 3. Computation (Chạy tính toán)

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/computation/` |
| **Body** | JSON |

**Đây là API quan trọng nhất**: chạy toàn bộ pipeline tính toán WPA cho 1 turbine và lưu kết quả xuống DB. Sau khi chạy thành công, tất cả các API phân tích khác mới có dữ liệu.

### Tham số

| Field | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | **Có** | — | Timestamp bắt đầu (ms). Khoảng thời gian ≥ 10 phút |
| `end_time` | int (ms) | **Có** | — | Timestamp kết thúc (ms) |
| `data_source` | string | Không | `"db"` | Nguồn dữ liệu: `"db"` hoặc `"file"` |
| `constants` | object | Không | `{}` | Override hằng số turbine (vd: `Swept_area`) |

### Trường hợp 1: Chạy cơ bản (lấy dữ liệu từ DB)

```json
{
  "start_time": 1325376000000,
  "end_time": 1356998400000
}
```

### Trường hợp 2: Chỉ định nguồn dữ liệu + override constants

```json
{
  "start_time": 1325376000000,
  "end_time": 1356998400000,
  "data_source": "db",
  "constants": {
    "Swept_area": 20000
  }
}
```

### Trường hợp 3: Lấy dữ liệu từ file

```json
{
  "start_time": 1325376000000,
  "end_time": 1356998400000,
  "data_source": "file"
}
```

### Response (200)

```json
{
  "success": true,
  "data": {
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "power_curves": { "global": [{"X": 0.5, "Y": 12.3}, ...] },
    "classification": {
      "summary": { "NORMAL": 0.65, "STOP": 0.15, "CURTAILMENT": 0.10, ... },
      "total_points": 52560
    },
    "indicators": {
      "AverageWindSpeed": 7.2,
      "RealEnergy": 12500.5,
      "LossEnergy": 1250.3,
      "CapacityFactor": 0.35,
      ...
    },
    "data_source_used": "db",
    "data_points_count": 52560,
    "constants_used": { "P_rated": 5000, "V_cutin": 3.0, "V_cutout": 25.0, "V_rated": 12.0, "Swept_area": 20000 },
    "units": { "WIND_SPEED": "m/s", "ACTIVE_POWER": "kW" },
    "computation_ids": {
      "classification": 1,
      "power_curve": 2,
      "weibull": 3,
      "indicators": 4,
      "yaw_error": 5
    }
  },
  "message": "Computation completed successfully"
}
```

### Test case lỗi

```json
// Thiếu start_time
{ "end_time": 1356998400000 }
// → 400, code: MISSING_PARAMETERS

// Khoảng thời gian < 10 phút
{ "start_time": 1325376000000, "end_time": 1325376300000 }
// → 400, code: INVALID_TIME_RANGE

// Turbine không tồn tại (URL: turbine_id=99999)
// → 404, code: TURBINE_NOT_FOUND
```

---

## 4. Turbine-level Analysis APIs

> **Yêu cầu**: Phải chạy Computation trước để có dữ liệu.

### 4.1 Classification Rate

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/classification-rate/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | Latest computation | Lọc theo computation time range |
| `end_time` | int (ms) | Không | Latest computation | Lọc theo computation time range |

#### TH1: Không tham số (lấy computation mới nhất)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/classification-rate/
```

#### TH2: Chỉ định time range

```
GET {{base_url}}/api/turbines/{{turbine_id}}/classification-rate/?start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "farm_name": "Farm A",
    "classification_rates": {
      "0": 0.652,
      "1": 0.153,
      "2": 0.045,
      "3": 0.098,
      "4": 0.012,
      "5": 0.030,
      "6": 0.010
    },
    "classification_map": {
      "0": "NORMAL",
      "1": "STOP",
      "2": "PARTIAL_STOP",
      "3": "CURTAILMENT",
      "4": "PARTIAL_CURTAILMENT",
      "5": "UNDER_PRODUCTION",
      "6": "OVER_PRODUCTION"
    }
  }
}
```

---

### 4.2 Indicators (Turbine)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/indicators/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | Latest computation | |
| `end_time` | int (ms) | Không | Latest computation | |

#### TH1: Không tham số

```
GET {{base_url}}/api/turbines/{{turbine_id}}/indicators/
```

#### TH2: Với time range

```
GET {{base_url}}/api/turbines/{{turbine_id}}/indicators/?start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "data": {
      "AverageWindSpeed": 7.23,
      "ReachableEnergy": 14520.5,
      "RealEnergy": 12500.3,
      "LossEnergy": 2020.2,
      "LossPercent": 13.91,
      "StopLoss": 850.1,
      "PartialStopLoss": 120.5,
      "UnderProductionLoss": 350.0,
      "CurtailmentLoss": 500.3,
      "PartialCurtailmentLoss": 199.3,
      "TotalStopPoints": 8000,
      "TotalPartialStopPoints": 2300,
      "TotalUnderProductionPoints": 1500,
      "TotalCurtailmentPoints": 5200,
      "DailyProduction": 34.25,
      "RatedPower": 5000,
      "CapacityFactor": 0.285,
      "Tba": 97.2,
      "Pba": 93.5,
      "Mtbf": 15.3,
      "Mttr": 2.1,
      "Mttf": 13.2,
      "YawMisalignment": -1.5,
      "UpPeriodsCount": 24,
      "DownPeriodsCount": 23,
      "UpPeriodsDuration": 340.5,
      "DownPeriodsDuration": 24.5,
      "AepWeibullTurbine": 16200.0,
      "AepWeibullWindFarm": 15800.0,
      "AepRayleighMeasured4": 9500.0,
      "AepRayleighMeasured5": 11200.0,
      "AepRayleighMeasured6": 12800.0,
      "AepRayleighMeasured7": 14100.0,
      "AepRayleighMeasured8": 15000.0,
      "AepRayleighMeasured9": 15500.0,
      "AepRayleighMeasured10": 15700.0,
      "AepRayleighMeasured11": 15800.0,
      "AepRayleighExtrapolated4": 9800.0,
      "AepRayleighExtrapolated11": 16100.0,
      "TimeStep": 600.0,
      "TotalDuration": 365.0,
      "DurationWithoutError": 354.0
    }
  }
}
```

---

### 4.3 Power Curve (Line + Scatter)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/power-curve/` |

Trả về **đường cong công suất** (power curve) và **scatter points** (điểm phân loại theo classification) để vẽ biểu đồ. Tỷ lệ classification và biểu đồ theo tháng lấy từ API classification-rate / cross-data-analysis nếu cần.

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Giá trị hợp lệ |
|-------|------|----------|---------|----------------|
| `mode` | string | Không | `"global"` | `global`, `time` |
| `time_type` | string | Nếu `mode=time` | — | `yearly`, `seasonally`, `monthly`, `day_night` |
| `start_time` | int (ms) | Không | Latest computation | |
| `end_time` | int (ms) | Không | Latest computation | |
| `max_points` | int | Không | `20000` | Giới hạn số điểm scatter (1000–200000) |

#### TH1: Global (mặc định)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/
```

hoặc tường minh:

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=global
```

**Response:**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "mode": "global",
    "time_type": null,
    "power_curve": [
      {"X": 0.5, "Y": 0.0},
      {"X": 1.0, "Y": 5.2},
      {"X": 1.5, "Y": 15.8},
      {"X": 2.0, "Y": 35.1}
    ],
    "points": {
      "group_by": "classification",
      "max_points": 20000,
      "data": [
        {"timestamp_ms": 1325376600000, "x": 5.2, "y": 1200.5, "group": "NORMAL"},
        {"timestamp_ms": 1325377200000, "x": 3.1, "y": 150.0, "group": "CURTAILMENT"}
      ]
    }
  }
}
```

#### TH2: Chia theo tháng

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=time&time_type=monthly
```

**Response:**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "mode": "time",
    "time_type": "monthly",
    "power_curve": {
      "1": [{"X": 0.5, "Y": 0.0}, {"X": 1.0, "Y": 5.5}],
      "2": [{"X": 0.5, "Y": 0.0}, {"X": 1.0, "Y": 4.8}]
    },
    "points": {
      "group_by": "classification",
      "max_points": 20000,
      "data": []
    }
  }
}
```

#### TH3: Chia theo mùa

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=time&time_type=seasonally
```

#### TH4: Chia ngày/đêm

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=time&time_type=day_night
```

#### TH5: Chia theo năm

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=time&time_type=yearly
```

#### TH6: Giới hạn scatter (max_points)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/?mode=global&max_points=10000
```

---

### 4.4 Weibull (Turbine)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/weibull/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default |
|-------|------|----------|---------|
| `start_time` | int (ms) | Không | Latest computation |
| `end_time` | int (ms) | Không | Latest computation |

#### TH1: Không tham số

```
GET {{base_url}}/api/turbines/{{turbine_id}}/weibull/
```

#### TH2: Với time range

```
GET {{base_url}}/api/turbines/{{turbine_id}}/weibull/?start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "data": {
      "A": 8.15,
      "K": 2.03,
      "Vmean": 7.23
    }
  }
}
```

> `A` = scale parameter (m/s), `K` = shape parameter, `Vmean` = mean wind speed (m/s).

---

### 4.5 Yaw Error

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/yaw-error/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default |
|-------|------|----------|---------|
| `start_time` | int (ms) | Không | Latest computation |
| `end_time` | int (ms) | Không | Latest computation |

```
GET {{base_url}}/api/turbines/{{turbine_id}}/yaw-error/
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "data": [
      {"X": 0.0, "Y": -2.5},
      {"X": 10.0, "Y": -1.8},
      {"X": 20.0, "Y": -0.5},
      {"X": 350.0, "Y": -3.1}
    ],
    "statistics": {
      "mean_error": -1.52,
      "median_error": -1.30,
      "std_error": 2.15
    }
  }
}
```

> `X` = wind direction bin (degrees), `Y` = mean yaw error (degrees).

---

### 4.6 Timeseries

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/timeseries/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Giá trị hợp lệ |
|-------|------|----------|---------|----------------|
| `sources` | string (lặp) | **Có** | — | `power`, `wind_speed`, `wind_direction`, `nacelle_direction`, `temperature`, `pressure`, `humidity` |
| `mode` | string | Không | `"raw"` | `raw`, `hourly`, `daily`, `monthly`, `seasonally`, `yearly` |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

> Truyền nhiều source: `sources=power&sources=wind_speed` hoặc lặp `sources`.

#### TH1: Raw power + wind_speed

```
GET {{base_url}}/api/turbines/{{turbine_id}}/timeseries/?sources=power&sources=wind_speed&start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

#### TH2: Resample theo giờ, chỉ wind_speed

```
GET {{base_url}}/api/turbines/{{turbine_id}}/timeseries/?sources=wind_speed&mode=hourly&start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

#### TH3: Resample theo ngày, nhiều source

```
GET {{base_url}}/api/turbines/{{turbine_id}}/timeseries/?sources=power&sources=wind_speed&sources=temperature&mode=daily
```

#### TH4: Resample theo tháng

```
GET {{base_url}}/api/turbines/{{turbine_id}}/timeseries/?sources=power&mode=monthly
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "mode": "raw",
    "units": {"wind_speed": "m/s", "power": "kW"},
    "data": [
      {"timestamp": 1325376000000, "power": 1200.5, "wind_speed": 7.2},
      {"timestamp": 1325376600000, "power": 1350.0, "wind_speed": 7.8}
    ]
  }
}
```

**Test case lỗi:**

```
// Không truyền sources
GET .../timeseries/
// → 400, code: MISSING_PARAMETERS

// Source không hợp lệ
GET .../timeseries/?sources=invalid_source
// → 400, code: INVALID_PARAMETERS
```

---

### 4.7 Working Period

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/working-period/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `variation` | int | Không | `50` | Ngưỡng variation (%): 1–100 |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

#### TH1: Mặc định (variation=50)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/working-period/
```

#### TH2: Variation thấp hơn (nhạy hơn)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/working-period/?variation=30
```

#### TH3: Với time range

```
GET {{base_url}}/api/turbines/{{turbine_id}}/working-period/?variation=50&start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "variation": 50,
    "units": {"performance": "%"},
    "data": [
      {"timestamp": 1325376000000, "performance": 95.2},
      {"timestamp": 1325376600000, "performance": 93.1}
    ]
  }
}
```

---

### 4.8 Wind Speed Analysis

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `bin_width` | float | Không | `1.0` | Độ rộng bin (m/s) |
| `threshold1` | float | Không | `4.0` | Ngưỡng tốc độ gió thấp (m/s) |
| `threshold2` | float | Không | `8.0` | Ngưỡng tốc độ gió cao (m/s) |
| `sectors_number` | int | Không | `16` | Số sectors: `4`, `8`, `12`, `16`, `24`, `36` |
| `mode` | string | Không | `"global"` | `global`, `time` |
| `time_type` | string | Nếu `mode=time` | — | `monthly`, `day_night`, `seasonally` |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

#### TH1: Global mặc định

```
GET {{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/
```

#### TH2: Tùy chỉnh bin + threshold

```
GET {{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/?bin_width=0.5&threshold1=3.0&threshold2=10.0&sectors_number=12
```

#### TH3: Chia theo tháng

```
GET {{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/?mode=time&time_type=monthly
```

#### TH4: Chia theo ngày/đêm

```
GET {{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/?mode=time&time_type=day_night
```

#### TH5: Chia theo mùa

```
GET {{base_url}}/api/turbines/{{turbine_id}}/wind-speed-analysis/?mode=time&time_type=seasonally
```

**Response (200) — TH1 global:**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "mode": "global",
    "time_type": null,
    "global_distribution": {
      "bin": [0.5, 1.5, 2.5, 3.5],
      "distribution": [0.02, 0.05, 0.08, 0.12]
    }
  }
}
```

---

### 4.9 Distribution

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/distribution/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `source_type` | string | Không | `"wind_speed"` | `wind_speed`, `power` |
| `bin_width` | float | Không | `1.0` (wind_speed) / `100.0` (power) | Độ rộng bin |
| `bin_count` | int | Không | `50` | Số bin tối đa |
| `mode` | string | Không | `"global"` | `global`, `time` |
| `time_type` | string | Nếu `mode=time` | — | `monthly`, `day_night`, `seasonally` |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

#### TH1: Phân bố tốc độ gió (global)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/distribution/
```

#### TH2: Phân bố công suất

```
GET {{base_url}}/api/turbines/{{turbine_id}}/distribution/?source_type=power
```

#### TH3: Phân bố gió, bin nhỏ hơn

```
GET {{base_url}}/api/turbines/{{turbine_id}}/distribution/?source_type=wind_speed&bin_width=0.5&bin_count=60
```

#### TH4: Chia theo tháng

```
GET {{base_url}}/api/turbines/{{turbine_id}}/distribution/?mode=time&time_type=monthly
```

#### TH5: Chia ngày/đêm cho power

```
GET {{base_url}}/api/turbines/{{turbine_id}}/distribution/?source_type=power&mode=time&time_type=day_night
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "farm_name": "Farm A",
    "mode": "global",
    "time_type": null,
    "source_type": "wind_speed",
    "global_distribution": {
      "bin": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
      "distribution": [0.02, 0.04, 0.07, 0.10, 0.12, 0.14]
    },
    "monthly_distribution": null,
    "statistics": {
      "vmean": 7.23,
      "vmax": 24.5,
      "vmin": 0.1
    }
  }
}
```

---

### 4.10 Static Table

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/static-table/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `source` | string (lặp) | Không | `["power", "wind_speed"]` | `wind_speed`, `power`, `wind_direction` |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

> Truyền nhiều source: `source=power&source=wind_speed&source=wind_direction`

#### TH1: Mặc định (power + wind_speed)

```
GET {{base_url}}/api/turbines/{{turbine_id}}/static-table/
```

#### TH2: Chỉ wind_direction

```
GET {{base_url}}/api/turbines/{{turbine_id}}/static-table/?source=wind_direction
```

#### TH3: Tất cả sources

```
GET {{base_url}}/api/turbines/{{turbine_id}}/static-table/?source=power&source=wind_speed&source=wind_direction
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "power": {
      "source": "power",
      "type": "ACTIVE_POWER",
      "statistics": {
        "average": 1520.3,
        "min": -5.0,
        "max": 5100.0,
        "standard_deviation": 1450.2,
        "start_date": 1325376000000,
        "end_date": 1356998400000,
        "possibale_records": 52560,
        "effective_records": 51200,
        "time_step": 600.0
      }
    },
    "wind_speed": {
      "source": "wind_speed",
      "type": "WIND_SPEED",
      "statistics": {
        "average": 7.23,
        "min": 0.0,
        "max": 24.5,
        "standard_deviation": 3.85,
        "start_date": 1325376000000,
        "end_date": 1356998400000,
        "possibale_records": 52560,
        "effective_records": 51800,
        "time_step": 600.0
      }
    }
  }
}
```

---

### 4.12 Time Profile

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/time-profile/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `sources` | string (lặp) | Không | `["power", "wind_speed"]` | `power`, `wind_speed`, `wind_direction`, `temperature`, `pressure`, `humidity` |
| `profile` | string | Không | `"hourly"` | `hourly`, `daily`, `monthly`, `seasonally` |
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |

#### TH1: Profil giờ, mặc định

```
GET {{base_url}}/api/turbines/{{turbine_id}}/time-profile/
```

#### TH2: Profil tháng

```
GET {{base_url}}/api/turbines/{{turbine_id}}/time-profile/?profile=monthly
```

#### TH3: Profil giờ, chỉ temperature

```
GET {{base_url}}/api/turbines/{{turbine_id}}/time-profile/?sources=temperature&profile=hourly
```

#### TH4: Profil mùa, nhiều sources

```
GET {{base_url}}/api/turbines/{{turbine_id}}/time-profile/?sources=power&sources=wind_speed&sources=temperature&profile=seasonally
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_name": "Farm A",
    "farm_id": 1,
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "sources": ["power", "wind_speed"],
    "profile": "hourly",
    "data": {
      "power": [
        {"hour": 0, "mean": 1100.5, "std": 800.2},
        {"hour": 1, "mean": 1050.3, "std": 790.5}
      ],
      "wind_speed": [
        {"hour": 0, "mean": 6.8, "std": 3.2},
        {"hour": 1, "mean": 6.5, "std": 3.1}
      ]
    }
  }
}
```

---

### 4.12 Cross Data Analysis (Turbine)

| | |
|---|---|
| **Method** | POST |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/cross-data-analysis/` |
| **Body** | JSON |

**Đây là API phức tạp nhất**, cho phép phân tích tương quan giữa 2 biến bất kỳ với nhiều tùy chọn lọc, nhóm, và hồi quy.

### Tham số (Body JSON)

| Field | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `x_source` | string | **Có** | — | Trục X: `power`, `wind_speed`, `wind_direction`, `nacelle_direction`, `temperature`, `pressure`, `humidity` |
| `y_source` | string | **Có** | — | Trục Y (cùng danh sách nguồn) |
| `group_by` | string | Không | `"none"` | Cách nhóm điểm (xem bảng dưới) |
| `max_points` | int | Không | `20000` | Giới hạn điểm trả về (1000–200000) |
| `regression` | object | Không | `{enabled: false}` | Cấu hình đường hồi quy |
| `regression.enabled` | bool | Không | `false` | Bật/tắt regression |
| `regression.type` | string | Không | `"linear"` | Loại regression (xem bảng dưới) |
| `regression.force_zero_intercept` | bool | Không | `false` | Bắt buộc qua gốc tọa độ |
| `only_computation_data` | bool | Không | `false` | Chỉ dùng dữ liệu đã tính (ClassificationPoint) |
| `include_statistics` | bool | Không | `false` | Trả thêm histogram + thống kê X/Y |
| `datetime` | object | Không | — | Bộ lọc thời gian |
| `datetime.start_time_ms` | int | Không | — | Timestamp bắt đầu (ms) |
| `datetime.end_time_ms` | int | Không | — | Timestamp kết thúc (ms) |
| `datetime.start_hour` | int | Không | — | Giờ bắt đầu (0–23) |
| `datetime.end_hour` | int | Không | — | Giờ kết thúc (0–23) |
| `filters` | object | Không | — | Bộ lọc dữ liệu |
| `filters.months` | int[] | Không | `[]` | Chỉ lấy tháng cụ thể: `[1,2,3]` = Jan–Mar |
| `filters.day_night` | string | Không | `""` | `"day"`, `"night"`, `""` (cả hai) |
| `filters.classifications` | string[] | Không | `[]` | Lọc trạng thái: `["NORMAL", "CURTAILMENT"]` |
| `filters.direction` | object | Không | `{}` | Lọc theo hướng gió |
| `filters.direction.source` | string | Không | — | `"wind_direction"` hoặc `"nacelle_direction"` |
| `filters.direction.sectors_number` | int | Không | 16 | Số sector: 4, 8, 12, 16, 24, 36 |
| `filters.direction.sectors` | int[] | Không | — | Chỉ giữ sector nào: `[0, 1, 2]` |
| `filters.ranges` | array | Không | `[]` | Lọc giá trị (xem TH8) |
| `group` | object | Không | — | Cấu hình khi `group_by=source` |
| `group.source` | string | Không | `""` | Source cho trục Z color gradient |
| `group.groups_count` | int | Không | `5` | Số bins (2–20) |
| `group.min` | float | Không | `null` | Giá trị min (auto nếu null) |
| `group.max` | float | Không | `null` | Giá trị max (auto nếu null) |

**Mapping tham số API ↔ Manual 1.3.6.2.7:** Data → `x_source`, `y_source`, `group_by`, `group.*`, `only_computation_data`, `filters.classifications`; Regression → `regression.type`, `regression.force_zero_intercept`; Date time → `datetime.start_time_ms`, `datetime.end_time_ms` (và start_hour/end_hour); Advanced filters → `filters.months`, `filters.day_night`, `filters.direction`, `filters.ranges`. Tham số mở rộng API (không có trong manual): `max_points` (giới hạn điểm trả về), `include_statistics` (tương đương "Show statistics chart" trong manual). `only_computation_data` = "Only computation data" trong manual.

**Giá trị `group_by` hợp lệ (turbine):**

| Giá trị | Mô tả |
|---------|-------|
| `none` | Không nhóm |
| `classification` | Nhóm theo trạng thái phân loại (NORMAL, STOP, ...) |
| `monthly` | Nhóm theo tháng+năm (2012-01, 2012-02, ...) |
| `yearly` | Nhóm theo năm (2012, 2013, ...) |
| `seasonally` | Nhóm theo mùa (Q1-2012, Q2-2012, ...) |
| `time_profile_monthly` | Nhóm theo tháng bất kể năm (Jan, Feb, ...) |
| `time_profile_seasonally` | Nhóm theo quý bất kể năm (Q1, Q2, Q3, Q4) |
| `source` | Nhóm theo giá trị source thứ 3 (Z-axis color gradient) |

**Giá trị `regression.type` hợp lệ:**

| Giá trị | Mô tả | Phương trình |
|---------|-------|-------------|
| `linear` | Hồi quy tuyến tính | y = ax + b |
| `polynomial2` | Đa thức bậc 2 | y = ax² + bx + c |
| `polynomial3` | Đa thức bậc 3 | y = ax³ + bx² + cx + d |
| `polynomial4` | Đa thức bậc 4 | y = ax⁴ + ... + e |
| `exponential` | Hàm mũ | y = a·eᵇˣ |
| `power` | Hàm lũy thừa | y = a·xᵇ |
| `logarithmic` | Hàm logarit | y = a·ln(x) + b |

---

#### TH1: Cơ bản — scatter wind_speed vs power

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "none",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 51200,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": null
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 6.8,
        "y": 1850.3,
        "group": null
      },
      {
        "timestamp_ms": 1325377200000,
        "x": 7.5,
        "y": 2450.0,
        "group": null
      }
    ]
  }
}
```

> **Kiểm tra**: `group_by` = "none", `group` = null cho mọi điểm, không có regression, không có statistics.

---

#### TH2: Nhóm theo classification + linear regression

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "group_by": "classification",
  "regression": { "enabled": true, "type": "linear" },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "classification",
    "regression": {
      "enabled": true,
      "type": "linear",
      "coefficients": [350.2, -450.5],
      "equation": "y = 350.20x + -450.50",
      "r2": 0.892,
      "rmse": 285.3
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 48500,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": "NORMAL"
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 3.1,
        "y": 150.0,
        "group": "CURTAILMENT"
      },
      {
        "timestamp_ms": 1325377200000,
        "x": 0.0,
        "y": 0.0,
        "group": "STOP"
      },
      {
        "timestamp_ms": 1325377800000,
        "x": 8.5,
        "y": 3200.0,
        "group": "NORMAL"
      }
    ]
  }
}
```

> **Kiểm tra**: `group_by` = "classification", mỗi điểm có `group` là "NORMAL", "STOP", "CURTAILMENT", v.v. Regression type = "linear", có coefficients [a, b] cho phương trình y = ax + b.

---

#### TH3: Polynomial regression bậc 3

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "regression": { "enabled": true, "type": "polynomial3" },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "none",
    "regression": {
      "enabled": true,
      "type": "polynomial3",
      "coefficients": [2.5, -15.3, 120.8, -200.5],
      "equation": "y = 2.50x³ + -15.30x² + 120.80x + -200.50",
      "r2": 0.956,
      "rmse": 185.2
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 51200,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": null
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 8.1,
        "y": 3100.0,
        "group": null
      }
    ]
  }
}
```

> **Kiểm tra**: Regression type = "polynomial3", coefficients có 4 phần tử [a, b, c, d] cho phương trình y = ax³ + bx² + cx + d. `group_by` = "none" (mặc định).

---

#### TH4: Nhóm theo tháng (time_profile_monthly)

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "group_by": "time_profile_monthly",
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "time_profile_monthly",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 51200,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": "Jan"
      },
      {
        "timestamp_ms": 1328054400000,
        "x": 6.8,
        "y": 1850.3,
        "group": "Feb"
      },
      {
        "timestamp_ms": 1330560000000,
        "x": 7.5,
        "y": 2450.0,
        "group": "Mar"
      },
      {
        "timestamp_ms": 1333238400000,
        "x": 8.1,
        "y": 3100.0,
        "group": "Apr"
      }
    ]
  }
}
```

> **Kiểm tra**: `group_by` = "time_profile_monthly", `group` là tên tháng viết tắt: "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec".

---

#### TH5: Nhóm theo source thứ 3 (Z-axis color)

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "group_by": "source",
  "group": {
    "source": "temperature",
    "groups_count": 5,
    "min": null,
    "max": null
  },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "source",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 51200,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": "15.0-20.0"
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 6.8,
        "y": 1850.3,
        "group": "20.0-25.0"
      },
      {
        "timestamp_ms": 1325377200000,
        "x": 7.5,
        "y": 2450.0,
        "group": "25.0-30.0"
      },
      {
        "timestamp_ms": 1325377800000,
        "x": 8.1,
        "y": 3100.0,
        "group": "30.0-35.0"
      }
    ]
  }
}
```

> **Kiểm tra**: `group_by` = "source", `group` là các bin của temperature dạng "min-max" (ví dụ: "15.0-20.0", "20.0-25.0"). Số bins = 5 như cấu hình.

---

#### TH6: Chỉ dữ liệu đã tính + lọc classification + thống kê

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "group_by": "classification",
  "only_computation_data": true,
  "include_statistics": true,
  "filters": {
    "classifications": ["NORMAL", "CURTAILMENT"]
  },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "classification",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 42000,
      "points_returned": 20000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": "NORMAL"
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 3.1,
        "y": 150.0,
        "group": "CURTAILMENT"
      },
      {
        "timestamp_ms": 1325377200000,
        "x": 8.5,
        "y": 3200.0,
        "group": "NORMAL"
      }
    ],
    "statistics": {
      "x": {
        "histogram": {
          "bins": [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24],
          "counts": [50, 200, 800, 1200, 1500, 1800, 2000, 1500, 1000, 500, 200, 50, 0]
        },
        "mean": 7.23,
        "std": 3.85,
        "min": 0.1,
        "max": 24.5,
        "median": 7.0,
        "count": 42000
      },
      "y": {
        "histogram": {
          "bins": [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000],
          "counts": [500, 800, 1200, 1500, 2000, 2500, 2000, 1500, 1000, 500, 0]
        },
        "mean": 1520.3,
        "std": 1450.2,
        "min": -5.0,
        "max": 5100.0,
        "median": 1350.0,
        "count": 42000
      }
    }
  }
}
```

> **Kiểm tra**: 
> - `only_computation_data` = true → chỉ dùng ClassificationPoint
> - `include_statistics` = true → có field `statistics` với histogram và thống kê X/Y
> - `filters.classifications` = ["NORMAL", "CURTAILMENT"] → chỉ có điểm với group là "NORMAL" hoặc "CURTAILMENT"
> - `rows_after_filters` < `rows_before_filters` do đã lọc classification

---

#### TH7: Lọc theo tháng + ngày/đêm

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "filters": {
    "months": [6, 7, 8],
    "day_night": "day"
  },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "none",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 12000,
      "points_returned": 12000
    },
    "points": [
      {
        "timestamp_ms": 1333238400000,
        "x": 6.5,
        "y": 1500.0,
        "group": null
      },
      {
        "timestamp_ms": 1335830400000,
        "x": 7.2,
        "y": 2100.5,
        "group": null
      },
      {
        "timestamp_ms": 1338508800000,
        "x": 8.1,
        "y": 2800.0,
        "group": null
      }
    ]
  }
}
```

> **Kiểm tra**: 
> - `filters.months` = [6, 7, 8] → chỉ lấy tháng 6, 7, 8 (Jun, Jul, Aug)
> - `filters.day_night` = "day" → chỉ lấy giờ ban ngày (6h-18h)
> - `rows_after_filters` giảm đáng kể do đã lọc theo tháng và ngày/đêm
> - Tất cả điểm có `group` = null (không group_by)

---

#### TH8: Lọc theo direction + range

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "filters": {
    "direction": {
      "source": "wind_direction",
      "sectors_number": 16,
      "sectors": [0, 1, 2, 3]
    },
    "ranges": [
      { "source": "wind_speed", "min": 3.0, "max": 25.0 },
      { "source": "power", "min": 0, "max": 5000 }
    ]
  },
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "none",
    "regression": {
      "enabled": false,
      "type": "linear",
      "coefficients": [],
      "equation": null,
      "r2": null,
      "rmse": null
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 15000,
      "points_returned": 15000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": null
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 8.1,
        "y": 3100.0,
        "group": null
      }
    ]
  }
}
```

> **Kiểm tra**: 
> - `filters.direction.sectors` = [0, 1, 2, 3] → chỉ lấy gió từ 4 sector đầu tiên (0-90 độ)
> - `filters.ranges` lọc wind_speed trong [3.0, 25.0] và power trong [0, 5000]
> - `rows_after_filters` giảm do đã lọc theo direction và range
> - Tất cả điểm có x trong [3.0, 25.0] và y trong [0, 5000]

---

#### TH9: Temperature vs pressure, exponential regression, force zero

**Request:**

```json
{
  "x_source": "temperature",
  "y_source": "pressure",
  "regression": {
    "enabled": true,
    "type": "exponential",
    "force_zero_intercept": false
  },
  "max_points": 5000,
  "datetime": {
    "start_time_ms": 1325376000000,
    "end_time_ms": 1356998400000
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "temperature",
    "y_source": "pressure",
    "group_by": "none",
    "regression": {
      "enabled": true,
      "type": "exponential",
      "coefficients": [95000.5, 0.0023],
      "equation": "y = 95000.50·e^(0.00230x)",
      "r2": 0.875,
      "rmse": 1250.8
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 51000,
      "points_returned": 5000
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 25.5,
        "y": 101325.0,
        "group": null
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 28.2,
        "y": 101350.5,
        "group": null
      },
      {
        "timestamp_ms": 1325377200000,
        "x": 30.1,
        "y": 101375.0,
        "group": null
      }
    ]
  }
}
```

> **Kiểm tra**: 
> - `x_source` = "temperature", `y_source` = "pressure" (khác wind_speed/power)
> - Regression type = "exponential", coefficients [a, b] cho phương trình y = a·e^(bx)
> - `force_zero_intercept` = false → không bắt buộc qua gốc
> - `max_points` = 5000 → `points_returned` ≤ 5000

---

#### TH10: Full options — regression + group + filter + stats

**Request:**

```json
{
  "x_source": "wind_speed",
  "y_source": "power",
  "group_by": "classification",
  "max_points": 20000,
  "regression": { "enabled": true, "type": "polynomial2", "force_zero_intercept": false },
  "only_computation_data": false,
  "include_statistics": true,
  "group": { "source": "", "groups_count": 5, "min": null, "max": null },
  "datetime": { "start_time_ms": 1325376000000, "end_time_ms": 1356998400000 },
  "filters": {
    "months": [1, 2, 3],
    "day_night": "",
    "classifications": ["NORMAL"],
    "direction": {},
    "ranges": []
  }
}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "x_source": "wind_speed",
    "y_source": "power",
    "group_by": "classification",
    "regression": {
      "enabled": true,
      "type": "polynomial2",
      "coefficients": [12.5, -30.2, 50.1],
      "equation": "y = 12.50x² + -30.20x + 50.10",
      "r2": 0.945,
      "rmse": 152.3
    },
    "period": {
      "start_time_ms": 1325376000000,
      "end_time_ms": 1356998400000
    },
    "summary": {
      "rows_before_filters": 52560,
      "rows_after_filters": 12500,
      "points_returned": 12500
    },
    "points": [
      {
        "timestamp_ms": 1325376000000,
        "x": 5.2,
        "y": 1200.5,
        "group": "NORMAL"
      },
      {
        "timestamp_ms": 1325376600000,
        "x": 8.1,
        "y": 3100.0,
        "group": "NORMAL"
      },
      {
        "timestamp_ms": 1328054400000,
        "x": 6.5,
        "y": 1850.3,
        "group": "NORMAL"
      }
    ],
    "statistics": {
      "x": {
        "histogram": {
          "bins": [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24],
          "counts": [80, 250, 700, 1000, 1300, 1600, 1800, 1500, 1000, 600, 300, 100, 20]
        },
        "mean": 7.23,
        "std": 3.85,
        "min": 0.1,
        "max": 24.5,
        "median": 7.0,
        "count": 12500
      },
      "y": {
        "histogram": {
          "bins": [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000],
          "counts": [600, 500, 800, 1200, 1500, 2000, 2500, 2000, 1000, 500, 300]
        },
        "mean": 1520.3,
        "std": 1450.2,
        "min": -5.0,
        "max": 5100.0,
        "median": 1350.0,
        "count": 12500
      }
    }
  }
}
```

> **Kiểm tra**: 
> - `group_by` = "classification" → tất cả điểm có `group` = "NORMAL" (do `filters.classifications` = ["NORMAL"])
> - `regression.type` = "polynomial2" → coefficients [a, b, c] cho y = ax² + bx + c
> - `include_statistics` = true → có field `statistics`
> - `filters.months` = [1, 2, 3] → chỉ lấy tháng 1, 2, 3 (Jan, Feb, Mar)
> - `filters.day_night` = "" → lấy cả ngày và đêm
> - `rows_after_filters` = 12500 < `rows_before_filters` = 52560 do đã lọc theo tháng và classification
> - `points_returned` = 12500 ≤ `max_points` = 20000

---

### Lưu ý về Response

Theo manual 1.3.6.2.7, response không echo `datetime`/`filters` và không trả `units`/source metadata. Cấu trúc response luôn có:
- `turbine_id`, `turbine_name`, `farm_id`, `farm_name`
- `x_source`, `y_source`, `group_by`
- `regression` (luôn có, enabled/disabled tùy request)
- `period` (start_time_ms, end_time_ms)
- `summary` (rows_before_filters, rows_after_filters, points_returned)
- `points` (mảng điểm với timestamp_ms, x, y, group)
- `statistics` (chỉ có khi `include_statistics` = true)

### Test case lỗi

```json
// Thiếu x_source / y_source
{ "y_source": "power" }
// → 400, code: INVALID_PARAMETERS

// Source không hợp lệ
{ "x_source": "invalid", "y_source": "power" }
// → 400, code: INVALID_PARAMETERS, message: "x_source and y_source must be in: humidity, nacelle_direction, power, pressure, temperature, wind_direction, wind_speed"

// group_by không hợp lệ
{ "x_source": "wind_speed", "y_source": "power", "group_by": "turbine" }
// → 400, code: INVALID_PARAMETERS (group_by turbine không hỗ trợ; API chỉ turbine-level)
```

---

### 4.13 Monthly Dashboard (Turbine)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/turbines/{{turbine_id}}/dashboard/monthly-analysis/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |
| `variation` | int | Không | `50` | Ngưỡng performance (1–100) |
| `indicators` | string (lặp) | Không | `[]` | KPI bổ sung (xem danh sách) |

**Giá trị `indicators` hợp lệ:**

`AverageWindSpeed`, `RealEnergy`, `LossEnergy`, `LossPercent`, `CapacityFactor`, `FailureCount`, `DailyProduction`, `ReachableEnergy`, `StopLoss`, `CurtailmentLoss`, `Tba`, `Pba`, `Mtbf`, `Mttr`, `Mttf`

> Truyền nhiều indicators: `indicators=RealEnergy&indicators=LossPercent` hoặc `indicators=RealEnergy,LossPercent`

#### TH1: Cơ bản, không indicators

```
GET {{base_url}}/api/turbines/{{turbine_id}}/dashboard/monthly-analysis/
```

#### TH2: Với indicators

```
GET {{base_url}}/api/turbines/{{turbine_id}}/dashboard/monthly-analysis/?indicators=RealEnergy&indicators=LossPercent&indicators=CapacityFactor
```

#### TH3: Với variation + time range

```
GET {{base_url}}/api/turbines/{{turbine_id}}/dashboard/monthly-analysis/?variation=30&start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200) — TH2:**

```json
{
  "success": true,
  "data": {
    "turbine_id": 1,
    "turbine_name": "WT1",
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "variation": 50,
    "series": {
      "monthly_production": [
        {"month_start_ms": 1325376000000, "production": 1200.5, "reachable": 1450.2, "loss": 249.7},
        {"month_start_ms": 1328054400000, "production": 1050.3, "reachable": 1280.1, "loss": 229.8}
      ],
      "monthly_performance": [
        {"month_start_ms": 1325376000000, "performance": 95.2},
        {"month_start_ms": 1328054400000, "performance": 88.1}
      ],
      "monthly_indicators": {
        "RealEnergy": [
          {"month_start_ms": 1325376000000, "value": 1200.5},
          {"month_start_ms": 1328054400000, "value": 1050.3}
        ],
        "LossPercent": [
          {"month_start_ms": 1325376000000, "value": 12.3},
          {"month_start_ms": 1328054400000, "value": 15.1}
        ],
        "CapacityFactor": [
          {"month_start_ms": 1325376000000, "value": 0.32},
          {"month_start_ms": 1328054400000, "value": 0.28}
        ]
      }
    },
    "selected_indicators": ["RealEnergy", "LossPercent", "CapacityFactor"]
  }
}
```

> **Ghi chú `monthly_production`**: Mỗi entry chứa 3 field production:
> - `production` = Real production (sản lượng thực tế)
> - `reachable` = Reachable production (sản lượng lý thuyết không tổn thất). `null` nếu dữ liệu cũ chưa chạy lại computation.
> - `loss` = Loss production = max(0, reachable - real). `null` nếu reachable là null.
>
> Frontend dùng 3 series này để vẽ biểu đồ PRODUCTION (stacked area chart). Để hiển thị dạng cumulative, frontend tính dồn: `cumulative[i] = sum(monthly[0..i])`.

---

## 5. Farm-level Analysis APIs

### 5.1 Indicators (Farm)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/indicators/` |

Không có tham số query (tổng hợp từ latest computation của mỗi turbine).

```
GET {{base_url}}/api/farms/{{farm_id}}/indicators/
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "data": {
      "AverageWindSpeed": 7.15,
      "ReachableEnergy": 145200.5,
      "RealEnergy": 125000.3,
      "LossEnergy": 20200.2,
      "LossPercent": 13.91,
      "DailyProduction": 342.5,
      "RatedPower": 50000,
      "CapacityFactor": 0.285,
      "Tba": 97.2,
      "Pba": 93.5
    }
  }
}
```

---

### 5.2 Weibull (Farm)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/weibull/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default |
|-------|------|----------|---------|
| `start_time` | int (ms) | Không | — |
| `end_time` | int (ms) | Không | — |

```
GET {{base_url}}/api/farms/{{farm_id}}/weibull/
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "data": {
      "A": 8.05,
      "K": 2.10,
      "Vmean": 7.15
    }
  }
}
```

---

### 5.3 Power Curve (Farm)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/power-curve/` |

**Tham số (Query):** Giống turbine power curve.

| Param | Type | Bắt buộc | Default |
|-------|------|----------|---------|
| `mode` | string | Không | `"global"` |
| `time_type` | string | Nếu `mode=time` | — |
| `start_time` | int (ms) | Không | — |
| `end_time` | int (ms) | Không | — |

#### TH1: Global

```
GET {{base_url}}/api/farms/{{farm_id}}/power-curve/
```

#### TH2: Monthly

```
GET {{base_url}}/api/farms/{{farm_id}}/power-curve/?mode=time&time_type=monthly
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "mode": "global",
    "time_type": null,
    "power_curves": [
      {
        "turbine_id": 1,
        "turbine_name": "WT1",
        "power_curve": [{"X": 0.5, "Y": 0.0}, {"X": 1.0, "Y": 5.2}]
      },
      {
        "turbine_id": 2,
        "turbine_name": "WT2",
        "power_curve": [{"X": 0.5, "Y": 0.0}, {"X": 1.0, "Y": 4.8}]
      }
    ]
  }
}
```

---

### 5.4 Failure Indicators (Histogram)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/failure-indicators/` |

Dùng cho biểu đồ histogram: Number of failures, MTTR, MTTF, MTBF per turbine.

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | Latest computation | Cả 2 phải đồng thời có hoặc không |
| `end_time` | int (ms) | Không | Latest computation | Cả 2 phải đồng thời có hoặc không |

#### TH1: Không tham số (latest)

```
GET {{base_url}}/api/farms/{{farm_id}}/failure-indicators/
```

#### TH2: Với time range

```
GET {{base_url}}/api/farms/{{farm_id}}/failure-indicators/?start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "turbines": [
      {"turbine_id": 1, "turbine_name": "WT1"},
      {"turbine_id": 2, "turbine_name": "WT2"}
    ],
    "indicators": {
      "FailureCount": [
        {"turbine_id": 1, "turbine_name": "WT1", "value": 23},
        {"turbine_id": 2, "turbine_name": "WT2", "value": 18}
      ],
      "Mttr": [
        {"turbine_id": 1, "turbine_name": "WT1", "value": 2.1},
        {"turbine_id": 2, "turbine_name": "WT2", "value": 1.8}
      ],
      "Mttf": [
        {"turbine_id": 1, "turbine_name": "WT1", "value": 13.2},
        {"turbine_id": 2, "turbine_name": "WT2", "value": 17.5}
      ],
      "Mtbf": [
        {"turbine_id": 1, "turbine_name": "WT1", "value": 15.3},
        {"turbine_id": 2, "turbine_name": "WT2", "value": 19.3}
      ]
    },
    "unit": {
      "Mttr": "days",
      "Mttf": "days",
      "Mtbf": "days"
    }
  }
}
```

> **Cách đọc**: Mỗi indicator là 1 mảng, mỗi phần tử = 1 turbine. Frontend render 4 histogram cạnh nhau.

**Test case lỗi:**

```
// Chỉ truyền 1 trong 2 (start_time mà không có end_time)
GET .../failure-indicators/?start_time=1325376000000
// → 400, code: INVALID_PARAMETERS
```

---

### 5.5 Failure Timeline (Gantt)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/failure-timeline/` |

Dùng cho biểu đồ Gantt chart: mỗi turbine 1 hàng, hiển thị các khoảng thời gian failure/stop trên trục thời gian (tháng J, F, M, A, M, ...).

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | Latest computation | Cả 2 phải đồng thời có hoặc không |
| `end_time` | int (ms) | Không | Latest computation | Cả 2 phải đồng thời có hoặc không |

#### TH1: Không tham số

```
GET {{base_url}}/api/farms/{{farm_id}}/failure-timeline/
```

#### TH2: Với time range

```
GET {{base_url}}/api/farms/{{farm_id}}/failure-timeline/?start_time={{start_time_ms}}&end_time={{end_time_ms}}
```

**Response (200):**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "months": [
      1325376000000,
      1328054400000,
      1330560000000,
      1333238400000,
      1335830400000,
      1338508800000,
      1341100800000,
      1343779200000,
      1346457600000,
      1349049600000,
      1351728000000,
      1354320000000
    ],
    "turbines": [
      {
        "turbine_id": 1,
        "turbine_name": "WT1",
        "events": [
          {
            "start_time": 1326075600000,
            "end_time": 1326094800000,
            "duration_s": 19200.0,
            "status": "STOP"
          },
          {
            "start_time": 1326180600000,
            "end_time": 1326246600000,
            "duration_s": 66000.0,
            "status": "STOP"
          }
        ]
      },
      {
        "turbine_id": 2,
        "turbine_name": "WT2",
        "events": [
          {
            "start_time": 1327392000000,
            "end_time": 1327422000000,
            "duration_s": 30000.0,
            "status": "STOP"
          }
        ]
      }
    ]
  }
}
```

> **Cách dùng cho Gantt chart:**
> - `months` = mảng timestamp đầu mỗi tháng → trục X (tick labels: J, F, M, A, M, J, J, A, S, O, N, D)
> - Mỗi turbine = 1 hàng (trục Y)
> - Mỗi event → 1 thanh ngang từ `start_time` đến `end_time`, màu theo `status`

---

### 5.6 Monthly Dashboard (Farm)

| | |
|---|---|
| **Method** | GET |
| **URL** | `{{base_url}}/api/farms/{{farm_id}}/dashboard/monthly-analysis/` |

**Tham số (Query):**

| Param | Type | Bắt buộc | Default | Mô tả |
|-------|------|----------|---------|-------|
| `start_time` | int (ms) | Không | — | |
| `end_time` | int (ms) | Không | — | |
| `variation` | int | Không | `50` | 1–100 |
| `indicators` | string (lặp) | Không | `[]` | Danh sách KPI cần trả thêm |

#### TH1: Cơ bản

```
GET {{base_url}}/api/farms/{{farm_id}}/dashboard/monthly-analysis/
```

#### TH2: Với indicators + variation

```
GET {{base_url}}/api/farms/{{farm_id}}/dashboard/monthly-analysis/?variation=40&indicators=RealEnergy&indicators=CapacityFactor&indicators=FailureCount
```

#### TH3: Với time range

```
GET {{base_url}}/api/farms/{{farm_id}}/dashboard/monthly-analysis/?start_time={{start_time_ms}}&end_time={{end_time_ms}}&indicators=LossPercent
```

**Response (200) — TH2:**

```json
{
  "success": true,
  "data": {
    "farm_id": 1,
    "farm_name": "Farm A",
    "start_time": 1325376000000,
    "end_time": 1356998400000,
    "variation": 40,
    "series": {
      "monthly_production": [
        {"month_start_ms": 1325376000000, "production": 12000.5, "reachable": 14500.2, "loss": 2499.7},
        {"month_start_ms": 1328054400000, "production": 10500.3, "reachable": 12800.1, "loss": 2299.8}
      ],
      "monthly_performance": [
        {"month_start_ms": 1325376000000, "performance": 94.5},
        {"month_start_ms": 1328054400000, "performance": 87.2}
      ],
      "monthly_indicators": {
        "RealEnergy": [
          {"month_start_ms": 1325376000000, "value": 12000.5},
          {"month_start_ms": 1328054400000, "value": 10500.3}
        ],
        "CapacityFactor": [
          {"month_start_ms": 1325376000000, "value": 0.32},
          {"month_start_ms": 1328054400000, "value": 0.28}
        ],
        "FailureCount": [
          {"month_start_ms": 1325376000000, "value": 5},
          {"month_start_ms": 1328054400000, "value": 3}
        ]
      }
    },
    "table": {
      "by_turbine": [
        {
          "turbine_id": 1,
          "turbine_name": "WT1",
          "monthly_production": [
            {"month_start_ms": 1325376000000, "production": 1200.5, "reachable": 1450.2, "loss": 249.7}
          ],
          "monthly_performance": [
            {"month_start_ms": 1325376000000, "performance": 95.2}
          ],
          "monthly_indicators": {
            "RealEnergy": [{"month_start_ms": 1325376000000, "value": 1200.5}],
            "CapacityFactor": [{"month_start_ms": 1325376000000, "value": 0.33}],
            "FailureCount": [{"month_start_ms": 1325376000000, "value": 2}]
          }
        }
      ]
    },
    "selected_indicators": ["RealEnergy", "CapacityFactor", "FailureCount"]
  }
}
```

> **Farm dashboard** có thêm `table.by_turbine` cho phép frontend hiển thị bảng chi tiết theo turbine.

Cross Data Analysis theo manual (1.3.6.2.7) **chỉ có ở turbine level**; không có API farm-level.

---

## 6. Negative Tests

Danh sách các trường hợp lỗi cần test cho mọi API:

### 6.1 Không có Authorization header

Gọi bất kỳ API nào mà không gắn token:

```
// Bỏ header Authorization
GET {{base_url}}/api/turbines/1/indicators/
// → 401 Unauthorized
```

### 6.2 Token hết hạn

```
// Dùng token cũ đã hết hạn
Authorization: Bearer eyJhbGciOi...expired...
// → 401 Unauthorized
```

### 6.3 Turbine không tồn tại

```
GET {{base_url}}/api/turbines/99999/indicators/
// → 404, code: TURBINE_NOT_FOUND
```

### 6.4 Farm không tồn tại

```
GET {{base_url}}/api/farms/99999/indicators/
// → 404, code: FARM_NOT_FOUND
```

### 6.5 Chưa chạy computation

```
// Turbine mới tạo, chưa có computation
GET {{base_url}}/api/turbines/{{turbine_id}}/power-curve/
// → 404, code: NO_COMPUTATION hoặc NO_RESULT_FOUND
```

### 6.6 Tham số bắt buộc bị thiếu

```
// Computation thiếu start_time
POST {{base_url}}/api/turbines/1/computation/
Body: { "end_time": 1356998400000 }
// → 400, code: MISSING_PARAMETERS

// Timeseries thiếu sources
GET {{base_url}}/api/turbines/1/timeseries/
// → 400, code: MISSING_PARAMETERS

// Cross data analysis thiếu x_source
POST {{base_url}}/api/turbines/1/cross-data-analysis/
Body: { "y_source": "power" }
// → 400, code: INVALID_PARAMETERS
```

### 6.7 Tham số sai giá trị

```
// mode không hợp lệ
GET {{base_url}}/api/turbines/1/power-curve/?mode=invalid
// → 400 hoặc fallback về global

// source_type không hợp lệ
GET {{base_url}}/api/turbines/1/distribution/?source_type=invalid
// → 400, code: INVALID_PARAMETERS

// time range quá ngắn (< 10 phút)
POST {{base_url}}/api/turbines/1/computation/
Body: { "start_time": 1325376000000, "end_time": 1325376300000 }
// → 400, code: INVALID_TIME_RANGE

// failure-indicators chỉ truyền 1 trong 2 timestamp
GET {{base_url}}/api/farms/1/failure-indicators/?start_time=1325376000000
// → 400, code: INVALID_PARAMETERS
```

### 6.8 Không có quyền truy cập

```
// User không có quyền xem farm/turbine của investor khác
GET {{base_url}}/api/turbines/5/indicators/
// → 403 Forbidden
```

---

## 7. Postman Collection

Trong repo đã có sẵn template collection + environment tại:

- `docs/postman/SmartWPA.postman_collection.json`
- `docs/postman/SmartWPA.postman_environment.json`

### Cách sử dụng

1. Import cả 2 file vào Postman
2. Chọn environment **SmartWPA**
3. Cập nhật `base_url` (mặc định `http://127.0.0.1:8000`)
4. Chạy request **Login** trước → script tự lưu token
5. Cập nhật `farm_id`, `turbine_id`, `start_time_ms`, `end_time_ms` theo dữ liệu thực tế
6. Chạy request **Computation** để tạo dữ liệu
7. Test các API phân tích theo thứ tự folder

### Thứ tự test khuyến nghị

```
1) Auth → Login
2) CRUD → Create Farm → Create Turbine
3) Computation → Chạy computation
4) Analysis (Turbine):
   → Classification Rate
   → Indicators
   → Power Curve (global → time, scatter points)
   → Weibull
   → Yaw Error
   → Timeseries (raw → hourly → daily)
   → Working Period
   → Wind Speed Analysis
   → Distribution
   → Static Table
   → Time Profile
   → Cross Data Analysis (từng TH)
   → Monthly Dashboard
5) Analysis (Farm):
   → Indicators
   → Weibull
   → Power Curve
   → Failure Indicators
   → Failure Timeline
   → Monthly Dashboard
6) Negative Tests
7) Auth → Logout
```
