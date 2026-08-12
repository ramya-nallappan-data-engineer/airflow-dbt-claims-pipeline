-- Intermediate layer: business logic lives here.
-- This is where we handle the duplicate-claim problem you'll see in
-- real claims feeds (resubmissions, batch reprocessing, etc.)

with staged as (

    select * from {{ ref('stg_claims') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by claim_id
            order by claim_date desc
        ) as rn

    from staged

)

select
    claim_id,
    patient_id,
    provider_id,
    provider_name,
    claim_date,
    procedure_code,
    diagnosis_code,
    billed_amount,
    paid_amount,
    claim_status

from deduped
where rn = 1
