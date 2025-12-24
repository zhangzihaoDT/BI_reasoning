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
