# 可用分析工具 (Analysis Tools)

定义了一组标准的商业智能 (BI) 分析动作。每个工具都是无状态的原子操作，不包含业务决策逻辑。

## 1. 基础查询 (Query)

- **Tool**: `query`
- **Desc**: 获取指定时间范围内、特定筛选条件下的单一指标数值。
- **Params**:
  - `metric`: 目标指标 (e.g., sales, order_count)
  - `date_range`: 时间范围
  - `filters`: 过滤条件 (optional)
- **Output**: 单一数值 (Scalar)

## 2. 维度拆解 (Rollup/Drilldown)

- **Tool**: `rollup`
- **Desc**: 按指定维度对指标进行分组聚合，用于查看不同维度的表现。
- **Params**:
  - `dimension`: 拆解维度 (e.g., model, city)
  - `metric`: 目标指标
  - `date_range`: 时间范围
  - `filters`: 过滤条件 (optional)
- **Output**: 维度-数值对列表 (List of {Dimension, Value})

## 3. 加法分解 (Additive Decomposition)

- **Tool**: `additive`
- **Desc**: 将一个总量指标分解为若干个部分的和，分析各部分对总量的贡献。
- **Params**:
  - `total_metric`: 总量指标
  - `components`: 组成部分列表 (List of Metrics/Dimensions)
  - `date_range`: 时间范围
- **Output**: 各部分数值及占比

## 4. 比率分解 (Ratio Decomposition)

- **Tool**: `ratio`
- **Desc**: 分析两个指标的比率关系 (分子/分母)，如转化率、客单价等。
- **Params**:
  - `numerator`: 分子指标
  - `denominator`: 分母指标
  - `date_range`: 时间范围
- **Output**: 比率值 (Percentage/Ratio)

## 5. 排名分析 (Top-N)

- **Tool**: `top_n`
- **Desc**: 获取某维度下指标排名靠前或靠后的项目。
- **Params**:
  - `dimension`: 排序维度
  - `metric`: 排序指标
  - `n`: 返回数量 (Default: 5)
  - `order`: 排序方式 (desc/asc)
  - `date_range`: 时间范围
- **Output**: 排序后的列表

## 6. 趋势分析 (Trend/YoY)

- **Tool**: `trend`
- **Desc**: 分析指标随时间的变化趋势，包括环比、同比等。
- **Params**:
  - `metric`: 目标指标
  - `time_grain`: 时间粒度 (day, week, month)
  - `compare_type`: 比较类型 (yoy, mom, wow, vs_avg)
  - `date_range`: 时间范围
- **Output**: 时间序列数据及增长率

## 7. 占比分析 (Composition)

- **Tool**: `composition`
- **Desc**: 计算某维度下的占比结构，用于饼图或树形图展示。
- **Params**:
  - `dimension`: 分解维度 (e.g., series_group, city)
  - `metric`: 指标 (e.g., order_count, sales)
  - `date_range`: 时间范围
  - `filters`: 过滤条件 (optional)
- **Output**: 列表 {dimension, value, percent}

## 8. 分布分析 (Distribution)

### 8.1 直方图 (Histogram)

- **Tool**: `histogram`
- **Desc**: 展示数值型指标的分布情况。
- **Params**:
  - `metric`: 数值指标 (e.g., invoice_amount, age)
  - `bins`: 分箱数量或自定义分箱 (e.g., 10 或 [0,100,500,1000])
  - `range`: 数值范围 (optional)
  - `filters`: 过滤条件 (optional)
- **Output**: 分箱区间与计数列表

### 8.2 箱线图 (Boxplot)

- **Tool**: `boxplot`
- **Desc**: 统计数值型变量的四分位分布，支持分组比较。
- **Params**:
  - `metric`: 数值指标
  - `group_by`: 分组维度 (optional)
  - `date_range`: 时间范围
  - `filters`: 过滤条件 (optional)
- **Output**: {min, q1, median, q3, max}（可按组输出）

### 8.3 帕累托图 (Pareto)

- **Tool**: `pareto`
- **Desc**: 对维度进行排序并计算累计占比，用于识别关键少数。
- **Params**:
  - `dimension`: 分解维度
  - `metric`: 指标
  - `top_n`: 可选，限制前 N 项 (optional)
  - `date_range`: 时间范围
- **Output**: 排序列表及累计占比 {dimension, value, cumulative_percent}

## 9. 相关性分析 (Correlation)

### 9.1 散点图 (Scatter)

- **Tool**: `scatter`
- **Desc**: 展示两个数值指标的相关性关系。
- **Params**:
  - `x_metric`: 横轴指标
  - `y_metric`: 纵轴指标
  - `group_by`: 分组维度 (optional)
  - `date_range`: 时间范围
  - `filters`: 过滤条件 (optional)
- **Output**: 点集 {x, y, group}

### 9.2 双轴趋势 (Dual Axis)

- **Tool**: `dual_axis`
- **Desc**: 同一时间轴展示两个不同指标的趋势，便于对比。
- **Params**:
  - `left_metric`: 左轴指标
  - `right_metric`: 右轴指标
  - `time_grain`: 时间粒度
  - `date_range`: 时间范围
- **Output**: 两组时间序列数据 {time, left_value, right_value}

## 10. 可视化映射建议 (Visualization Mapping)

- 排序/排名 → `top_n` → 条形图（水平/垂直）
- 趋势对比 → `trend` / `dual_axis` → 折线图（支持同比/环比/双轴）
- 占比结构 → `composition` → 饼图 / 树形图（Treemap）
- 文本表格 → `rollup` → 表格（维度-数值列表）
- 分布形态 → `histogram` / `boxplot` → 直方图 / 箱线图
- 关键少数 → `pareto` → 帕累托图（按累计占比排序）
- 相关性探索 → `scatter` → 散点图（可按组着色）
