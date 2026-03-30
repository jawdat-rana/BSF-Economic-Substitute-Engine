## Project summary

The **BSF Economic Substitution Engine** is a lightweight dashboard prototype that tracks whether Black Soldier Fly-related products are becoming more economically competitive against conventional agricultural inputs.

This first version focuses on:

* **Urea**
* **DAP**
* **Fishmeal**
* an early **BSF meal benchmark band**
* and an **AU vs global fertilizer comparison**

The goal is to turn fragmented market references into a more usable view for substitution analysis.

## Data sources used

### 1. World Bank Commodity Price Data (“Pink Sheet”)

Used for monthly historical series for:

* **Urea**
* **DAP**
* **Fishmeal**

This is the main global benchmark source in the dashboard.

### 2. GrainGrowers Fertiliser Reports

Used for Australia-specific **urea** observations from recent reports.

For the current prototype, these are captured from recent GrainGrowers fertiliser reports and normalized into a monthly AU benchmark series using the **latest report available in each month**.

### 3. Monthly FX assumptions

AU urea report values are published in **AUD**, while the global benchmark series is in **USD**.
To compare them directly, monthly **AUD → USD average FX rates** were applied.

## What is currently available on the dashboard

### 1. Latest snapshot cards

These show the most recent available values in the selected range for:

* Global Urea
* AU Urea
* Urea Delta
* Fishmeal
* Fishmeal vs BSF midpoint benchmark

### 2. Delta Tracker

A line chart comparing:

* **Global Urea**
* **AU Urea**

This shows when the Australian urea market is trading at a premium or discount versus the global benchmark.

### 3. Monthly Urea Premium / Discount

A bar chart showing:

* **AU Urea – Global Urea**

This makes the monthly premium or discount easy to scan.

### 4. Protein Pivot

A chart comparing:

* **Fishmeal**
* **BSF meal benchmark band**

This is meant to show how fishmeal pricing is moving relative to an early BSF competitiveness corridor.

### 5. Insight cards

Rule-based commentary is generated from the derived datasets to summarize notable monthly conditions.

## Key assumptions used

### 1. AU monthly urea value

GrainGrowers reports can appear multiple times in the same month.
For the current prototype, the dashboard uses the **latest available report in each month** as the monthly AU point.

### 2. Currency normalization

AU urea values are converted from **AUD to USD** using monthly average FX rates.

### 3. BSF meal price band

The current **BSF meal benchmark band is a modeled placeholder**, not a live traded market feed.

For this version, a fixed benchmark corridor was used:

* Low: **$1,200/t**
* Mid: **$1,500/t**
* High: **$1,800/t**

This is intended as an early comparison aid, not a definitive market price.

### 4. Coverage differences across datasets

The World Bank series has a long historical range, while the AU urea series currently covers only recent months.
Because of this, comparative visuals are filtered to periods where both sides are available.

## Current limitations

* The AU fertilizer side is currently strongest for **urea**; DAP is not yet consistently available in the same structured way from the chosen AU source.
* The BSF side is still based on **benchmarks and assumptions**, not a transparent market ticker.
* Insight generation is currently **rule-based**, not yet a full narrative intelligence layer.
* This is a **decision-support prototype**, not a production-grade market intelligence platform yet.

## What this prototype is useful for

It is useful for:

* quickly understanding AU vs global fertilizer pricing gaps
* exploring whether fishmeal is moving closer to BSF competitiveness levels
* testing the overall dashboard concept with real users
* identifying where the data model is strong and where more source coverage is needed
