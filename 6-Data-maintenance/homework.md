---

# Data Engineering Pipeline Management Plan

This document outlines the proposed management plan for the team's data pipelines, covering ownership, on-call responsibilities, and key runbooks for investor-facing metrics.

---

## 1. Pipeline Ownership

To ensure clear accountability and deep domain knowledge, each pipeline has been assigned a primary and secondary owner. The primary owner is responsible for the day-to-day health of the pipeline, while the secondary owner acts as a backup and subject matter expert.

| Business Area | Pipeline Name | Primary Owner | Secondary Owner |
| :--- | :--- | :--- | :--- |
| **Profit** | Profit (for experiments) | Alice | Bob |
| | Aggregate Profit (investor) | Bob | Charlie |
| **Growth** | Aggregate Growth (investor) | Charlie | Diana |
| | Daily Growth (for experiments) | Diana | Alice |
| **Engagement** | Aggregate Engagement (investor) | Alice | Bob |

---

## 2. On-Call Schedule

This on-call schedule is designed for fairness, with each engineer taking a week of primary on-call duty every four weeks. In the event of a holiday falling on an on-call week, the individual on-call may swap with their secondary owner for that week.

| Week | Primary On-Call |
| :--- | :--- |
| **Week 1** | Alice |
| **Week 2** | Bob |
| **Week 3** | Charlie |
| **Week 4** | Diana |

*Note: This schedule rotates continuously. For example, in Week 5, the primary on-call reverts to Alice.*

---

## 3. Runbooks for Investor Reporting Pipelines

These runbooks detail potential failure points for the critical pipelines that feed into investor reports. The goal is to provide a reference for quick and accurate problem identification.

### A. Aggregate Profit

This pipeline ingests and aggregates transaction and cost data to calculate overall profit margins.

**Potential Failure Points:**
* **Source Data Incompleteness:** Upstream financial data systems fail to deliver all transaction records, leading to an underreported profit figure.
* **Data Latency:** The ETL process for cost data is delayed, causing the profit calculation to be performed with outdated information.
* **Schema Drift:** A new field is added or an existing field is changed in the source transaction data, causing the pipeline's calculation logic to fail or produce incorrect results.
* **Malicious Data Injection:** Malformed or fraudulent transaction records are introduced into the source system, corrupting the aggregate profit number.
* **Calculation Logic Error:** A bug in the aggregation query, such as an incorrect join or a misapplied filter, results in an inaccurate final metric.

### B. Aggregate Growth

This pipeline tracks user signups and active user counts to measure overall business growth.

**Potential Failure Points:**
* **Tracking Event Failures:** The front-end tracking system fails to send user signup events, leading to an underreporting of new users.
* **Incorrect Time Windows:** The pipeline's query logic uses the wrong time window (e.g., UTC vs. local time), causing discrepancies in the daily or weekly growth numbers.
* **Duplicate User IDs:** The source system creates duplicate user IDs, which are incorrectly counted multiple times by the pipeline, leading to an overreported growth figure.
* **Bot or Spam Traffic:** An influx of bot signups or non-human activity is not properly filtered out, inflating the growth metrics.
* **Infrastructure Outage:** A database or processing cluster outage prevents the pipeline from running on schedule, leaving the growth report stale.

### C. Aggregate Engagement

This pipeline processes user behavior events (clicks, scrolls, views) to report on user engagement metrics like daily active users (DAUs) or session length.

**Potential Failure Points:**
* **Event Log Corruption:** The raw event logs are corrupted or improperly formatted, causing the pipeline to skip or fail to process key user actions.
* **Sessionization Logic Errors:** The logic used to group events into a single "session" breaks, leading to inaccurate session counts and average session lengths.
* **Changes in Product UI:** A change in the front-end user interface causes event names to be altered, but the pipeline's logic is not updated to reflect this, leading to zero-reported engagement for new features.
* **High Cardinality Data:** The volume of new event types or user identifiers exceeds the pipeline's capacity, causing processing to slow down or fail.
* **Data Dropping:** Events are dropped by the ingestion system before they reach the pipeline, resulting in an underreporting of engagement.