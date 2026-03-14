-- Test: Assets must equal Liabilities + Equity within 1%
-- Returns rows that fail the test (any result = test failure)

with balance_sheet as (
    select
        ticker,
        company_name,
        period_end,
        max(case when metric = 'Assets' then value_usd end)              as assets,
        max(case when metric = 'Liabilities' then value_usd end)         as liabilities,
        max(case when metric = 'StockholdersEquity' then value_usd end)  as equity
    from {{ ref('fct_financials') }}
    where metric in ('Assets', 'Liabilities', 'StockholdersEquity')
    group by ticker, company_name, period_end
),

checked as (
    select *,
        abs(assets - (liabilities + equity)) / nullif(assets, 0) as imbalance_pct
    from balance_sheet
    where assets is not null
    and liabilities is not null
    and equity is not null
)

select *
from checked
where imbalance_pct > 0.01