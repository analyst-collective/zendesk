with 
reply_times as
(
    select
        ticket_id, date_trunc('week',ticket_date)::date as ticket_week,
        datediff(min, ticket_date, first_reply_date)/60.0 hours_to_first_reply
    from {{ref("zendesk_ticket_summary")}}
    where
        -- limit to this year's tickets that we solved
        ticket_date > '2016-01-01'
        and first_reply_date is not null
)

-- find average and median times to first reply
select distinct
    ticket_week, 
    avg(hours_to_first_reply) over(partition by ticket_week) as avg_hours_to_first_reply,
    median(hours_to_first_reply) over(partition by ticket_week) as med_hours_to_first_reply
from reply_times
order by ticket_week