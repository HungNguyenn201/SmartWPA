I/ Cơ chế license

┌─────────────────────────────────────────┐
│           ADMIN (Main Admin)            │
│  - Tạo License cho Investor              │
│  - Gia hạn License                       │
│  - Vô hiệu hóa License                   │
│  - Làm License vĩnh viễn                 │
└─────────────────────────────────────────┘
                    │
                    │ Quản lý
                    ▼
┌─────────────────────────────────────────┐
│           INVESTOR                       │
│  - Có License riêng                     │
│  - Không thể tự gia hạn                  │
│  - Quản lý Farms của mình                │
│  - Tạo Farm Admin/Staff                  │
└─────────────────────────────────────────┘
                    │
                    │ Sở hữu
                    ▼
┌─────────────────────────────────────────┐
│              FARM                        │
│  - Thuộc về Investor                    │
│  - Dùng License của Investor            │
└─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌──────────────┐      ┌──────────────┐
│ FARM ADMIN   │      │    STAFF     │
│ - Dùng       │      │ - Dùng       │
│   License    │      │   License    │
│   của        │      │   của        │
│   Investor   │      │   Investor   │
└──────────────┘      └──────────────┘



