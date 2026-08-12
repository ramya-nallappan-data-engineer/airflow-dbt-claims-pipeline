with claims as (

    select * from {{ ref('int_claims_deduped') }}

)

select
    provider_id,
    count(*)                                              as total_claims,
    sum(case when claim_status = 'PAID' then 1 else 0 end)    as paid_claims,
    sum(case when claim_status = 'DENIED' then 1 else 0 end)  as denied_claims,
    sum(case when claim_status = 'PENDING' then 1 else 0 end) as pending_claims,
    sum(billed_amount)                                    as total_billed,
    sum(paid_amount)                                      as total_paid,
    round(
        sum(paid_amount) / nullif(sum(billed_amount), 0) * 100,
        2
    )                                                      as pct_of_billed_recovered

from claims
group by provider_id
