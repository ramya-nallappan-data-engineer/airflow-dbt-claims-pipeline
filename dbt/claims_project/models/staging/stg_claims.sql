-- Staging layer: 1:1 with the source, light cleaning only.
-- No business logic here — just casting, renaming, trimming.

with source as (

    select * from {{ ref('raw_claims') }}

),

cleaned as (

    select
        claim_id,
        patient_id,
        provider_id,
        trim(provider_name)                    as provider_name,
        cast(claim_date as date)                as claim_date,
        procedure_code,
        nullif(trim(diagnosis_code), '')        as diagnosis_code,
        cast(billed_amount as numeric(10,2))    as billed_amount,
        cast(paid_amount as numeric(10,2))      as paid_amount,
        upper(trim(claim_status))               as claim_status

    from source

)

select * from cleaned
