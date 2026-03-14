-- Joins company metadata with financial facts into company-period grain

with companies as (
    select * from {{ ref('stg_submissions') }}
),

facts as (
    select * from {{ ref('stg_company_facts') }}
),

joined as (
    select
        -- Company identifiers
        f.ticker,
        f.cik,
        c.company_name,
        c.sic_code,
        c.sic_description,
        c.industry_group,
        c.fiscal_year_end,

        -- Period
        f.period_start,
        f.period_end,
        f.period_days,
        f.form,
        f.filed_date,
        f.accession_number,

        -- Metric
        f.metric,
        f.value_usd,
        f.frame,

        -- Metadata
        f._loaded_at

    from facts f
    left join companies c
        on f.ticker = c.ticker
)

select * from joined