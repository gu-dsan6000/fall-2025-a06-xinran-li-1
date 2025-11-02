## 1. Cluster & Application Analysis

### Key Findings
- The dataset contains **6 independent compute clusters**, but resource usage is highly uneven.
- One cluster (`1485248649253`) carries the vast majority of the workload.

### Detailed Analysis

| Metric | Value/Description |
| :--- | :--- |
| **Total Applications** | 194 |
| **Average Applications per Cluster** | ~32.33 |
| **Most Active Cluster** | `1485248649253` (181 applications) |
| **Other Clusters** | Each ≤ 8 applications |

### Conclusions and Inferences
- **Severe resource imbalance**: Cluster `1485248649253` runs 181 applications, accounting for **93.3%** of the total (181/194). This indicates that this cluster is the primary workload carrier in the production environment, while the other 5 clusters are almost idle or used only for testing/temporary tasks.
- **Significant lifecycle differences**:
  - The largest cluster (`1485248649253`) has a runtime span of up to **6 months** (from 2017-01-24 to 2017-07-27), representing a long-term stable production cluster.
  - Several smaller clusters (e.g., `1440487435730`, `1460011102909`) ran only single short-lived jobs, likely for one-off tasks or debugging.
  - Cluster `1474351042505` ran applications for over 24 hours, representing typical long-running batch jobs.
- **Application startup patterns**:
  - In the later period of cluster `1485248649253` (June 2017), a large number of applications were started consecutively within a very short time (<1 second) (e.g., apps 0150 to 0171). This is very likely **an indication of a scheduler system (such as Airflow or Oozie) executing a workflow containing many subtasks**. These applications may be dependent on each other and triggered sequentially or in parallel.

---

## 2. Log Analysis

### Key Findings
- The total log volume is huge, but relevant information is highly concentrated.
- Log level distribution is extremely unbalanced, with `INFO` dominating overwhelmingly.

### Detailed Analysis

| Metric | Value/Description |
| :--- | :--- |
| **Total Log Lines** | 33,236,604 |
| **Lines Containing Log Levels** | 27,410,250 |
| **Unique Log Levels** | 3 (`INFO`, `WARN`, `ERROR`) |
| **Log Level Distribution** | `INFO`: 99.92%, `ERROR`: 0.04%, `WARN`: 0.04% |


## 3. Comprehensive Analysis and Recommendations

### Key Insights
1. **Single point of failure risk**: Cluster `1485248649253` is the core of the entire system; its stability and performance directly determine the success or failure of all business operations. If this cluster fails, nearly all services would be interrupted.
2. **Trade-off between log cost and efficiency**: The current logging strategy records excessive detail, which aids in-depth debugging but sacrifices storage efficiency and slows problem diagnosis.
3. **Evidence of automated scheduling**: The dense startup of applications within specific time windows is a clear sign of automated workflow execution.