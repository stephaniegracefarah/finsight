-- Cleans raw EDGAR financial facts and filters to true quarterly values only

with source as (
    select * from {{ source('raw', 'raw_company_facts') }}
),

cleaned as (
    select
        ticker,
        cast(cik as varchar)                           as cik,
        metric,
        cast(val as double)                            as value_usd,
        cast(start as date)                            as period_start,
        cast("end" as date)                              as period_end,
        form,
        cast(filed as date)                            as filed_date,
        accn                                           as accession_number,
        frame,

        -- Calculate the number of days this value covers
        datediff('day',
            cast(start as date),
            cast("end" as date)
        )                                              as period_days,

        current_timestamp                              as _loaded_at

    from source
    -- Only keep quarterly and annual filings
    where form in ('10-Q', '10-K')
    -- Must have both start and end dates
    and start is not null
    and "end" is not null
),

quarterly_only as (
    select *
    from cleaned
    where
        -- True quarters are ~90 days (allow 60-105 day window)
        -- This filters out the cumulative YTD values that were
        -- causing duplicates in LYV revenue data
        period_days between 60 and 105
),

deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by ticker, metric, period_end
                order by filed_date desc
            ) as rn
        from quarterly_only
    )
    where rn = 1
)

select * from deduplicated