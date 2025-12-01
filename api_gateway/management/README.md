# Management Module Structure

Cấu trúc module đã được refactor để dễ bảo trì và mở rộng.

## Cấu trúc

```
api_gateway/management/
├── __init__.py              # Export tất cả views (backward compatibility)
├── common/                  # Code dùng chung
│   ├── __init__.py
│   ├── helpers.py           # Helper functions và exceptions (create_or_get_investor, UserValidationError, FarmValidationError)
│   └── validators.py         # Validators chung (validate_email, validate_name, etc.)
│
├── auth/                    # Authentication & Authorization
│   ├── __init__.py
│   ├── views.py             # Login, Logout, Token Refresh
│   └── helpers.py           # Token helpers (get_token_for_user, check_license)
│
├── license/                 # License Management
│   ├── __init__.py
│   └── views.py             # License CRUD
│
├── users/                   # User Management
│   ├── __init__.py
│   ├── views.py             # User CRUD views (11 views)
│   └── validators.py        # User-specific validators
│
├── farms/                   # Farm Management
│   ├── __init__.py
│   ├── views.py             # Farm CRUD views (5 views)
│   ├── validators.py        # Farm-specific validators
│   └── helpers.py           # Farm-specific helpers
│
├── turbines/                # Turbine Management
│   ├── __init__.py
│   ├── views.py             # Turbine CRUD views (5 views)
│   ├── validators.py        # Turbine-specific validators
│   └── helpers.py           # Turbine-specific helpers
│
└── acquisition/              # Acquisition Management (SmartHIS, PointType, HISPoint)
    ├── __init__.py
    ├── views.py             # Acquisition CRUD views (15 views)
    ├── validators.py        # Acquisition-specific validators
    └── helpers.py           # Acquisition-specific helpers
```

## Cách sử dụng

### Import từ module cụ thể (Recommended)

```python
from api_gateway.management.auth import UserLoginView, TokenRefreshView
from api_gateway.management.users import UserListAPIView, UserInfoView
from api_gateway.management.farms import FarmCreateAPIView, FarmListAPIView
from api_gateway.management.turbines import TurbineCreateAPIView, TurbineListAPIView
from api_gateway.management.acquisition import SmartHISCreateAPIView, PointTypeListAPIView, HISPointCreateAPIView
```

### Import từ management (Backward compatibility)

```python
from api_gateway.management import UserLoginView, UserListAPIView, FarmCreateAPIView
```

## Migration từ cấu trúc cũ

### Trước đây:
```python
from api_gateway.management.users_management import UserLoginView, UserListAPIView
from api_gateway.management.farm_management import FarmCreateAPIView
```

### Bây giờ:
```python
from api_gateway.management.auth import UserLoginView
from api_gateway.management.users import UserListAPIView
from api_gateway.management.farms import FarmCreateAPIView
```

Hoặc sử dụng backward compatibility:
```python
from api_gateway.management import UserLoginView, UserListAPIView, FarmCreateAPIView
```

## Lợi ích

1. **Tách biệt trách nhiệm**: Mỗi module chỉ làm 1 việc
2. **Dễ tìm code**: Code liên quan được nhóm lại
3. **Tái sử dụng**: Helper functions chung ở `common/`
4. **Dễ mở rộng**: Thêm module mới không ảnh hưởng module cũ
5. **Dễ test**: Test từng module độc lập

## Files cũ (có thể xóa sau khi test)

- `users_management.py` (1304 dòng) - Đã được tách thành:
  - `auth/views.py` (Login, Logout, Token Refresh)
  - `license/views.py` (License Management)
  - `users/views.py` (User CRUD)
  
- `farm_management.py` (458 dòng) - Đã được tách thành:
  - `farms/views.py` (Farm CRUD)
  
- `turbines_management.py` (573 dòng) - Đã được tách thành:
  - `turbines/views.py` (Turbine CRUD)

