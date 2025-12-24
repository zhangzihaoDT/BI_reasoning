# Parquet 文件路径

parquet*file = "/Users/zihao*/Documents/coding/dataset/formatted/intention_order_analysis.parquet"

## 数据集定义

### 1. 时间维度 (Time Dimensions)

用于按时间段（日、周、月、年）进行趋势分析和筛选。

- `Order_Create_Time`: 订单创建时间
- `store_create_date`: 门店创建日期
- `Deposit_Payment_Time`: 大定支付时间
- `Invoice_Upload_Time`: 发票上传时间
- `Intention_Payment_Time`: 意向金支付时间
- `intention_refund_time`: 意向金退款时间
- `deposit_refund_time`: 大定退款时间
- `first_assign_time`: 首次分配时间
- `Lock_Time`: 锁单时间
- `first_touch_time`: 首次接触时间

### 2. 可用指标 (Metrics)

用于计算总和、平均值、计数等数值指标。

- **锁单量**: `Order Number` 计数 (条件: `Lock_Time` 非空)
- **交付数**: `Order Number` 计数 (条件: `Lock_Time` AND `Invoice_Upload_Time` 非空)
- **小订数**: `Order Number` 计数 (条件: `Intention_Payment_Time` 非空)
- `开票价格`: 销售额/单价 (Invoice Price)
- `owner_age`: 车主年龄
- `buyer_age`: 购买人年龄

### 3. 可用维度 (Dimensions)

用于分组、筛选和拆解分析。

#### 产品与车型

- `Product Name`: 产品名称
- `车型分组`: 车型分组
- `pre_vehicle_model_type`: 预购车型类型

#### 地理位置

- `Store City`: 门店城市
- `Parent Region Name`: 大区名称
- `License Province`: 上牌省份
- `License City`: 上牌城市
- `license_city_level`: 上牌城市等级

#### 渠道与门店

- `Store Name`: 门店名称
- `Store Code`: 门店代码
- `first_main_channel_group`: 首次主渠道分组
- `Store Agent Name`: 门店代理人姓名
- `Store Agent Id`: 门店代理人 ID
- `Store Agent Phone`: 门店代理人电话

#### 客户信息

- `owner_gender`: 车主性别
- `order_gender`: 订单性别
- `Owner Cell Phone`: 车主手机号
- `Owner Identity No`: 车主身份证号
- `Buyer Cell Phone`: 购买人手机号
- `Buyer Identity No`: 购买人身份证号

#### 其他

- `Order Number`: 订单号

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
