with claims as (

    select * from {{ ref('int_claims_deduped') }}

)

select distinct
    provider_id,
    provider_name

from claims
