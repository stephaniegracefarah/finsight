-- Test: Revenue should never be negative
-- Returns rows that fail the test (any result = test failure)

select
    ticker,
    company_name,
    period_end,
    value_usd
from {{ ref('fct_financials') }}
where metric = 'Revenues'
and value_usd < 0