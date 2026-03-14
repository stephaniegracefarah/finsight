-- models/staging/stg_submissions.sql
-- Cleans and standardizes company metadata from SEC EDGAR submissions

with source as (
    select * from {{ source('raw', 'raw_submissions') }}
),

renamed as (
    select
        ticker,
        cik,
        entity_name                                    as company_name,
        cast(sic as integer)                           as sic_code,
        sic_description,
        category,
        fiscal_year_end,
        state_of_incorporation,

        -- Derived fields
        case
            when sic_description ilike '%amusement%'
              or sic_description ilike '%entertainment%'
              or sic_description ilike '%recreation%'
            then 'Live Entertainment'
            when sic_description ilike '%services%'
            then 'Services'
            else 'Other'
        end                                            as industry_group,

        current_timestamp                              as _loaded_at

    from source
)

select * from renamed