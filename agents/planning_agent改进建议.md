这个输出**非常有代表性**，而且恰好暴露了你现在 PlanningAgent 的**关键“认知缺口”**。
我们可以把它当成一次**诊断样本**，来精确校正 PlanningAgent 的职责与能力边界。

---

## 一、先客观评价当前输出（不是“错”，而是“太窄”）

你现在的 PlanningAgent 做的是：

```json
{
  "action": "query",
  "metric": "昨日销量",
  "date": "yesterday",
  "reasoning": "直接回答问题"
}
```

### 这在逻辑上是：

- ✅ **语义正确**
- ✅ **最小可行**
- ❌ **不具备分析师思维**
- ❌ **不具备 BI reasoning**

一句话评价：

> **这是一个“搜索式回答”，不是“分析式评估”。**

---

## 二、为什么这是“不够的”（但很多 Agent 都停在这里）

因为你问的是：

> **“昨日销量如何？”**

而不是：

> “昨日销量是多少？”

在人类分析师语境中：

- “多少” → 单点事实查询
- **“如何” → 评估、对照、判断**

👉 **PlanningAgent 误把“如何”退化成了“是多少”**

这是当前所有 LLM Planning 的**系统性问题**，不是你一个人的问题。

---

## 三、缺的到底是什么？——缺的是「评估语义」

我们用一句**非常关键的判断标准**来界定：

> **只要问题中隐含“状态判断 / 好坏 / 异常 / 是否正常”，
> PlanningAgent 就不应该只输出 1 条 DSL。**

### “昨日销量如何？”隐含了哪些评估维度？

一个合格分析师**默认会同时对照**：

1. 自身对照

   - vs 前一日
   - vs 前一周

2. 时间尺度对照

   - vs 30 日均值
   - vs 月均值 / 年均值

3. 结构对照

   - 分车型
   - 分品牌

4. 历史对照

   - vs 去年同期

👉 这些**不是 drilldown**
👉 是 **evaluation scan（评估扫描）**

---

## 四、PlanningAgent 在这一步的“正确职责”

### ❌ 错误职责（你现在的）：

> “我直接回答问题”

### ✅ 正确职责：

> **“我要构造一个最小但完整的评估视角集合”**

也就是你前面已经说对的那句话：

> **人类分析师不是一个 DSL，而是一组 DSL 在做判断**

---

## 五、把当前输出「升级」为合格的 Planning 输出

我们不推翻你现在的结构，只做**增强**。

### 原始输出（你现在）

```json
[
  {
    "tool": "query",
    "metric": "昨日销量"
  }
]
```

### 合格的「广度扫描版」Planning 输出（示例）

```json
[
  {
    "step_id": 1,
    "tool_name": "query",
    "parameters": {
      "metric": "Order Number 不同计数",
      "date_range": "yesterday"
    },
    "reasoning": "Establish baseline: yesterday's total sales."
  },
  {
    "step_id": 2,
    "tool_name": "ratio",
    "parameters": {
      "numerator": "yesterday_sales",
      "denominator": "day_minus_1_sales"
    },
    "reasoning": "Evaluate short-term change by comparing with previous day."
  },
  {
    "step_id": 3,
    "tool_name": "ratio",
    "parameters": {
      "numerator": "yesterday_sales",
      "denominator": "day_minus_7_sales"
    },
    "reasoning": "Evaluate weekly pattern stability."
  },
  {
    "step_id": 4,
    "tool_name": "ratio",
    "parameters": {
      "numerator": "yesterday_sales",
      "denominator": "rolling_30d_avg_sales"
    },
    "reasoning": "Compare against recent baseline to assess abnormality."
  },
  {
    "step_id": 5,
    "tool_name": "ratio",
    "parameters": {
      "numerator": "yesterday_sales",
      "denominator": "last_year_same_day_sales"
    },
    "reasoning": "Assess long-term seasonal context."
  },
  {
    "step_id": 6,
    "tool_name": "rollup",
    "parameters": {
      "dimension": "model",
      "metric": "Order Number 不同计数",
      "date_range": "yesterday"
    },
    "reasoning": "Check structural composition to detect concentration or missing segments."
  }
]
```

注意几个**非常重要的变化**：

---

## 六、这里体现出的「PlanningAgent 进阶认知」

### 1️⃣ DSL 不再是“回答”，而是「评估视角」

每一条 DSL 对应的不是：

- 一个事实

而是：

- 一个 **判断依据**

---

### 2️⃣ reasoning 字段不再是“解释工具选择”

你现在的 reasoning 是：

> “我用 query 因为问题问销量”

这是**初级 reasoning**。

升级后的 reasoning 是：

> “我需要这个视角，来判断状态是否异常 / 好坏”

👉 reasoning = **为什么这个评估视角是必要的**

---

### 3️⃣ PlanningAgent 已经在「模拟分析师脑内 checklist」

这一步非常关键：

- 它还没 drilldown
- 也没解释结论
- 但已经在**搭建判断所需的信息结构**

---

## 七、一个你可以直接落到 Prompt / Code 里的原则

你可以给 PlanningAgent 加一条**硬约束规则**：

> **当问题属于「状态评估类」而非「数值查询类」时，
> 必须生成 ≥ N 条 DSL，覆盖：**
>
> - 当前值
> - 至少 2 个时间对照
> - 至少 1 个结构视角

这一步一旦加上，你的 Agent 会**质变**。

---

## 八、总结一句（非常重要）

> **你现在不是要“让 PlanningAgent 更聪明”，
> 而是要让它从“回答问题”升级为“构造判断依据”。**

这一关过了，后面的：

- AnalysisAgent
- 回环
- 自动 drilldown

才有意义。

---

如果你愿意，下一步我可以直接帮你做一件**非常工程化的事**：

👉 **给 PlanningAgent 设计一套「问题类型 → 广度扫描 DSL 模板」的映射表**
比如：

- 状态评估类
- 异常解释类
- 对比决策类

这一步，会让你的系统真正像一个**资深分析师**，而不是“会查数的 Copilot”。

太好了，这一步**就是你整个系统从「能跑」迈向「像分析师」的分水岭**。
下面我给你一套 **可直接落地到 PlanningAgent 的「问题类型 → 广度扫描 DSL 模板映射表」**，不是概念版，而是**工程可用版**。

---

# 一、先给总览：PlanningAgent 的「问题类型地图」

PlanningAgent **第一步不是生成 DSL**，而是做一个**轻量级意图分类**：

```
User Question
   ↓
Question Type Classifier
   ↓
DSL Template Selector
   ↓
DSL Parameter Filler
```

---

# 二、核心问题类型 → 对应 DSL 模板

我先给你 **最重要、最常用的 5 类**（足够支撑 80% BI 场景）

---

## ① 状态评估类（Evaluation）

### 🔹 典型问题

- 昨日销量如何？
- 本周表现怎么样？
- 当前销售情况正常吗？

### 🔹 分析师真实意图

> **判断“好 / 坏 / 正常 / 异常”**

### 🔹 DSL 生成原则（硬约束）

- 当前值（baseline）
- ≥2 个时间对照
- ≥1 个结构视角
- 不 drilldown

---

### ✅ DSL 模板（Evaluation）

```yaml
evaluation:
  baseline:
    - tool: query
      metric: sales
      date: target_date
  time_comparison:
    - tool: ratio
      compare_to: prev_day
    - tool: ratio
      compare_to: prev_week
    - tool: ratio
      compare_to: rolling_30d_avg
  structural_scan:
    - tool: rollup
      dimension: primary_dimension # brand / model
      metric: sales
```

---

## ② 趋势判断类（Trend）

### 🔹 典型问题

- 销量是在上涨还是下滑？
- 最近走势如何？

### 🔹 分析师真实意图

> **判断方向 & 稳定性**

### 🔹 DSL 生成原则

- 时间序列
- 平滑 / 均线
- 不关注结构细节（先）

---

### ✅ DSL 模板（Trend）

```yaml
trend:
  series:
    - tool: timeseries
      metric: sales
      window: last_30_days
  smoothing:
    - tool: moving_avg
      window: 7d
  comparison:
    - tool: ratio
      compare_to: prev_period
```

---

## ③ 异常解释类（Explain / Why）

### 🔹 典型问题

- 为什么销量下降？
- 哪些因素导致增长？

### 🔹 分析师真实意图

> **找驱动因素**

### 🔹 DSL 生成原则

- 必须有 baseline
- additive / ratio 分解
- drilldown 合法

---

### ✅ DSL 模板（Explain）

```yaml
explain:
  baseline:
    - tool: query
      metric: sales
  decomposition:
    - tool: additive_decompose
      by: primary_dimension # channel / brand
    - tool: ratio_decompose
      by: secondary_dimension # store / city
  drilldown:
    enabled: true
```

---

## ④ 对比决策类（Compare）

### 🔹 典型问题

- A 和 B 哪个更好？
- 不同品牌谁表现更优？

### 🔹 分析师真实意图

> **支持选择 / 排序**

### 🔹 DSL 生成原则

- 同口径
- 排名
- 差值 / 比率

---

### ✅ DSL 模板（Compare）

```yaml
compare:
  baseline:
    - tool: rollup
      dimension: compare_dimension
      metric: sales
  ranking:
    - tool: rank
      metric: sales
  gap_analysis:
    - tool: ratio
```

---

## ⑤ 监控预警类（Monitor）

### 🔹 典型问题

- 今天有没有异常？
- 哪些指标需要关注？

### 🔹 分析师真实意图

> **发现问题，而不是解释问题**

---

### ✅ DSL 模板（Monitor）

```yaml
monitor:
  scan:
    - tool: query
      metric: sales
  threshold_check:
    - tool: flag
      rule: deviation_from_avg
  alert_dimension:
    - tool: rollup
      dimension: risk_dimension
```

---

# 三、PlanningAgent 的「执行顺序逻辑」

你可以直接固化成规则：

```text
1. classify(question)
2. select_template(question_type)
3. bind_parameters(schema / ads / time)
4. generate_dsl_sequence
```

---

# 四、一个关键设计原则（非常重要）

> **PlanningAgent 永远不做 drilldown 决策，
> 只负责“把该看的都先看一遍”。**

drilldown 只能由：

- AnalysisAgent
- 或 Execution 后的条件判断触发

---

# 五、你现在这个系统的「质变点」

一旦你把这套映射表接入：

- ❌ PlanningAgent = 会查数
- ✅ PlanningAgent = **构造判断依据的分析规划器**

你现在这个输出：

```json
[ query(yesterday_sales) ]
```

会自动升级为：

```json
[ baseline + time_compare + structure_scan ]
```

而且 **不依赖模型“灵光一现”**。

---

# 六、下一步（强烈建议）

下一步我建议你做的不是加 Agent，而是：

👉 **把这 5 类模板写成 YAML / JSON 配置文件**
👉 PlanningAgent 只负责：

- 识别类型
- 填参数
- 输出 DSL

如果你愿意，**下一条我可以直接帮你：**

- 写一个 `planning_templates.yaml`
- 或把这套逻辑直接映射到你现有 `planning_agent.py` 的伪代码

你选一个，我直接给你可用版本。
