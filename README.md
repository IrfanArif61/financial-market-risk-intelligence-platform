# Multi-Asset Portfolio Risk Analysis

> **Business question:** *If you held an equal slice of six very different assets — the S&P 500, Apple, Tesla, Bitcoin, Ethereum and gold — how much risk are you actually carrying, where does it come from, and which assets earned their risk?*

Acting as a risk analyst, I built a monitoring platform over **~3 years of daily market data (Mar 2023 – Mar 2026) across 6 assets** spanning equities, crypto, an index and a commodity. The goal isn't to pick winners — it's to answer the question every risk desk asks: *is this portfolio as diversified as it looks, and is the risk going where we think it is?*

---

## Executive summary

On paper this is a strong, "diversified" portfolio — an **equal-weight (16.67% each)** mix that returned **44.6%** and beat the S&P 500 benchmark over the window. But three findings show the diversification is largely an illusion, and the risk is concentrated exactly where you'd least want it:

- **Equal weight is not equal risk.** Every asset holds 16.67% of the capital, yet **Ethereum and Bitcoin alone contribute ~55% of total portfolio risk** (ETH 29.8%, BTC 25.1%). Two of the six assets, a third of the money, drive over half the volatility.
- **The "diversification" barely diversifies.** Correlations across the basket run **0.52 to 0.96** — Bitcoin–Ethereum move together at **0.96**, the S&P–Apple at **0.95**, S&P–Tesla at **0.91**. In a real sell-off, five of the six assets would fall together. **Only gold sits apart** (correlations 0.52–0.63) — the one genuine hedge in the book.
- **The headline-return winners were the risk-adjusted losers.** Bitcoin (+214%) and Tesla (+118%) topped the return table — but they also delivered the *worst* drawdowns (ETH −64%, TSLA −54%, BTC −50%) and weak Sharpe ratios. The quiet winners on a **risk-adjusted basis were gold and the S&P** (highest Sharpe, shallowest drawdowns).

**The so-what:** the portfolio's risk is concentrated in crypto, and its only real diversifier is gold. Trimming crypto weight would cut portfolio volatility far more than the capital reduction implies — and on a risk-adjusted view, the "boring" assets did the real work.

---

## Northstar metrics (Mar 2023 – Mar 2026)

| Metric | Value |
|---|---|
| Portfolio return | 44.6% |
| Portfolio volatility (30-day) | 2.25% |
| Portfolio Value at Risk (95%) | −2.21% |
| Average asset Sharpe | 0.07 |
| Assets monitored | 6 (equity, crypto, index, commodity) |

---

## Insights deep-dive

### 1. Returns vs risk-adjusted returns — they tell opposite stories
![Executive overview](screenshots/executive_overview.png)

Bitcoin (+214%), gold (+181%) and Tesla (+118%) led raw cumulative returns. But ranked by **Sharpe ratio** (return per unit of risk), the order flips: **GLD 0.127 and ^GSPC 0.123 sit top**, while the headline winners languish — **ETH 0.026, TSLA 0.048, BTC 0.051**. On 30-day volatility, TSLA (3.6%) and ETH (3.3%) are the most violent, gold (1.0%) and the S&P (0.8%) the calmest. **Lesson:** a big return number says nothing about whether the ride was worth it.

### 2. Equal weight ≠ equal risk — crypto dominates the risk budget
![Portfolio construction](screenshots/portfolio_construction.png)

The portfolio is allocated equally (16.67% per asset), but the **risk contribution** is anything but equal: **ETH 29.8%, BTC 25.1%, GLD 18.0%, TSLA 12.1%, AAPL 10.7%, ^GSPC 4.4%**. The two crypto positions supply **~55% of portfolio risk** off a third of the capital. The equal-weight portfolio *did* beat the S&P benchmark over the window — but an investor who thought they were "balanced" was in fact running a crypto-dominated risk profile. (The efficient-frontier view is an approximation across these 6 assets, used to demonstrate the risk/return trade-off, not to prescribe an optimal allocation.)

### 3. Diversification is weaker than it looks — gold is the only hedge
![Risk analytics](screenshots/risk_analytics.png)

The correlation matrix is the most important chart in the project. **Everything is positively correlated, mostly strongly:** BTC–ETH **0.96**, ^GSPC–AAPL **0.95**, ^GSPC–TSLA **0.91**, AAPL–TSLA **0.85**. A basket where assets move together at 0.7–0.96 offers little protection when markets turn. **Gold is the standout exception** — its correlations to everything else sit at **0.52–0.63**, the lowest in the matrix, which is exactly why it also posts the shallowest drawdown (−13.9%). Portfolio VaR (95%) sat around −2% day-to-day but spiked toward −4.5% in the early-2025 stress window.

### 4. Drawdowns — where the "winners" actually hurt
![Market analysis](screenshots/market_analysis.png)

The full per-asset picture makes the risk-vs-reward trade explicit:

| Asset | Class | Return | 30D Volatility | VaR (95%) | Max Drawdown |
|---|---|---|---|---|---|
| BTC-USD | Crypto | +213.7% | 4.38% | −4.19% | **−49.7%** |
| GLD | Commodity ETF | +180.9% | 3.14% | −4.26% | −13.9% |
| TSLA | Equity | +118.0% | 2.11% | −3.29% | **−53.8%** |
| ^GSPC | Index | +68.8% | 0.77% | −1.28% | −18.9% |
| AAPL | Equity | +68.4% | 1.86% | −2.79% | −33.4% |
| ETH-USD | Crypto | +29.0% | 5.21% | −5.51% | **−63.8%** |

Ethereum returned just 29% but put investors through a **−64% peak-to-trough** loss — the worst risk-for-reward in the book. Gold delivered a comparable headline return to the equity names with a fraction of the drawdown. This is the table that reframes "best performer" into "best *investment*."

---

## Recommendations (tied to the numbers)

1. **Re-weight away from crypto if the goal is balance.** ETH + BTC contribute ~55% of risk from 33% of capital; cutting crypto weight reduces portfolio volatility disproportionately.
2. **Treat gold as the portfolio's hedge, not just a return source.** Its 0.52–0.63 correlations and −13.9% max drawdown make it the only genuine diversifier in this basket.
3. **Judge assets on Sharpe and drawdown, not headline return.** On that basis GLD and the S&P were the strongest holdings; ETH and TSLA the weakest despite TSLA's +118%.
4. **Add a true diversifier.** Because everything here correlates at 0.5+, the book needs an asset with low/negative correlation (e.g. bonds, defensives) to materially cut tail risk.

---

## How the analysis was built *(technical appendix)*

<details>
<summary>Pipeline, metrics, and engineering detail (click to expand)</summary>

**Stack:** Python → PostgreSQL → Power BI.

- **Data:** ~3 years of daily prices for 6 assets pulled via the yfinance API (^GSPC, AAPL, TSLA, BTC-USD, ETH-USD, GLD), ~5,000 observations.
- **Warehouse:** layered PostgreSQL design — raw (`dim_assets`, `fact_market_prices`) → Python analytics layer → reporting tables (`fact_risk_metrics`, `fact_portfolio_metrics`, `fact_rolling_correlations`) → semantic SQL views for BI.
- **Metrics computed in Python:** daily & cumulative returns, 7/30-day rolling volatility, **Value at Risk (95%)**, rolling **Sharpe ratio**, drawdown & max drawdown, rolling pairwise **correlations**, and portfolio-level aggregates.
- **Dashboard:** 4-page Power BI suite (Executive Overview, Risk Analytics, Portfolio Construction, Market Analysis).
- **Code:** `/python` (extraction, transformation, analytics, database); **SQL:** `/sql`.

**Scope & caveats:** this is a **risk-monitoring** project, not investment advice. The 6-asset basket is a fixed, illustrative, equal-weighted set on public data; the efficient-frontier page is an approximation demonstrating the method, not an optimised mandate. Returns are price-only (no dividends/fees) over a single ~3-year window, so figures are sensitive to the chosen period.
</details>

---

**Irfan Arif** — Data Analyst · MSc FinTech, BSc Computer Science
[LinkedIn](https://linkedin.com/in/irfanarif7) · [GitHub](https://github.com/IrfanArif61)
