# NYC 311 Weekly Trends (Auto-updating when re-run)

## Question
What are the top NYC 311 complaint types in the last 7 days, and which are rising fastest vs the prior week?

## Data Source
NYC Open Data (Socrata) — 311 Service Requests (2020–present)

## Method
- Pull aggregated counts by `complaint_type` for the last 7 days and the prior 7 days using SoQL
- Compute week-over-week deltas
- Plot:
  1) Top 20 complaint types (last 7 days)
  2) Biggest increases/decreases vs prior week
  3) Daily trend for top 5 complaint types (last 30 days)

## How to Run
Open the notebook and run all cells. Re-running on a later date will update results.

## Example Findings (latest run)
- HEAT/HOT WATER dominated winter complaint volume.
- Snow or Ice showed a major week-over-week spike (weather-driven).
- Noise and Illegal Parking declined vs last week.
