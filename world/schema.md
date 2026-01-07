# Parquet 文件路径

source*report = "/Users/zihao*/Documents/coding/dataset/original/order_data_report.md"

## 数据集定义

### A. 外部下发数据集 (Assign Data)

- 路径: `assign_file = Path("/Users/zihao*/Documents/coding/dataset/original/assign_data.csv")`
- 时间维度:
  - `Assign Time 年/月/日`
- 指标:
  - `下发线索数`
  - `下发线索当日试驾数`
  - `下发线索 7 日试驾数`
  - `下发线索 7 日锁单数`
  - `下发线索 30日试驾数`
  - `下发线索 30 日锁单数`
  - `下发门店数`

### 1. 时间维度 (Time Dimensions)

用于按时间段（日、周、月、年）进行趋势分析和筛选，字段来自源报表。

- `order_create_time`: 订单创建时间
- `order_create_date`: 订单创建日期
- `store_create_date`: 门店创建日期
- `lock_time`: 锁单时间
- `invoice_upload_time`: 发票上传时间
- `delivery_date`: 交付日期
- `intention_payment_time`: 意向金支付时间
- `intention_refund_time`: 意向金退款时间
- `deposit_payment_time`: 大定支付时间
- `deposit_refund_time`: 大定退款时间
- `apply_refund_time`: 申请退款时间
- `approve_refund_time`: 审批退款时间
- `first_touch_time`: 首次接触时间
- `first_test_drive_time`: 首次试驾时间
- `lead_assign_time_max`: 线索最大下发时间
- `first_assign_time`: 首次下发时间

### 2. 可用指标 (Metrics)

用于计算总和、平均值、计数等数值指标，字段名与数据源一致。

- **锁单量**: `order_number` 计数 (条件: `lock_time` 非空)
- **交付数**: `order_number` 计数 (条件: `lock_time` AND `delivery_date` 非空 或 `invoice_upload_time` 非空)
- **小订数**: `order_number` 计数 (条件: `intention_payment_time` 非空)
- **开票金额**: `invoice_amount`
- **年龄**: `age`
- **试驾次数**: `td_countd`
- **订单计数**: `order_number` 计数

### 3. 可用维度 (Dimensions)

用于分组、筛选和拆解分析，字段名与数据源一致。

#### 产品与车型

- `product_name`: 产品名称
- `series_group`: 车型分组（派生维度，依据 [business_definition.json](file:///Users/zihao_/Documents/github/W52_reasoning/world/business_definition.json) 的 `series_group_logic` 生成）
- `series`: 车型系列
- `belong_intent_series`: 意向系列
- `drive_series_cn`: 驱动系列（中文）

#### 地理位置

- `store_city`: 门店城市
- `parent_region_name`: 大区名称
- `license_province`: 上牌省份
- `license_city`: 上牌城市
- `license_city_level`: 上牌城市等级

#### 渠道与门店

- `store_name`: 门店名称
- `first_middle_channel_name`: 首次中间渠道名称

#### 客户信息

- `gender`: 性别
- `age`: 年龄
- `is_staff`: 是否员工
- `is_hold`: 是否保留

#### 其他

- `order_number`: 订单号
- `order_type`: 订单类型
- `main_lead_id`: 主线索 ID
- `finance_product`: 金融产品
- `final_payment_way`: 尾款支付方式

### 4. 业务规则 (Business Rules)

结合分析工具 (`tool.md`) 定义以下业务逻辑规则：

#### 趋势分析 (Trend Analysis)

使用 `trend` 工具进行比较分析。

- **短期趋势 (Short-term Trend)**

  - 环比前一日 (vs Previous Day)
  - 环比前一周 (vs Previous Week)
  - 环比前一月 (vs Previous Month)

- **长期趋势 (Long-term Trend)**
  - 对比 30 日均值 (vs 30-day Average)
  - 对比全年均值 (vs Yearly Average)
  - 对比去年均值 (vs Last Year Average)
