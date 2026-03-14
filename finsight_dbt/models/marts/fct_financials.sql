-- Final analytical table with QoQ and YoY change columns
-- This is the table the dashboard and anomaly detection read from

with base as (
    select * from {{ ref('int_financials') }}
),

with_changes as (
    select
        -- Identifiers
        ticker,
        cik,
        company_name,
        sic_code,
        sic_description,
        industry_group,
        fiscal_year_end,

        -- Period
        period_start,
        period_end,
        period_days,
        form,
        filed_date,
        accession_number,
        frame,

        -- Metric
        metric,
        value_usd,

        -- Quarter over Quarter change
        lag(value_usd) over (
            partition by ticker, metric
            order by period_end
        )                                               as prev_quarter_value,

        value_usd - lag(value_usd) over (
            partition by ticker, metric
            order by period_end
        )                                               as qoq_change,

        case
            when lag(value_usd) over (
                partition by ticker, metric
                order by period_end
            ) = 0 then null
            else round(
                (value_usd - lag(value_usd) over (
                    partition by ticker, metric
                    order by period_end
                )) / abs(lag(value_usd) over (
                    partition by ticker, metric
                    order by period_end
                )) * 100,
            2)
        end                                             as qoq_pct_change,

        -- Year over Year change (4 quarters back)
        lag(value_usd, 4) over (
            partition by ticker, metric
            order by period_end
        )                                               as prev_year_value,

        value_usd - lag(value_usd, 4) over (
            partition by ticker, metric
            order by period_end
        )                                               as yoy_change,

        case
            when lag(value_usd, 4) over (
                partition by ticker, metric
                order by period_end
            ) = 0 then null
            else round(
                (value_usd - lag(value_usd, 4) over (
                    partition by ticker, metric
                    order by period_end
                )) / abs(lag(value_usd, 4) over (
                    partition by ticker, metric
                    order by period_end
                )) * 100,
            2)
        end                                             as yoy_pct_change,

        -- Metadata
        _loaded_at

    from base
)

select * from with_changes
order by ticker, metric, period_end