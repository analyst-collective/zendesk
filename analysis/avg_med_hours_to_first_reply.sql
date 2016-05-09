with 
reply_times as
(
    select
        ticket_id, date_trunc('week',ticket_date)::date as ticket_week,
        datediff(mins, ticket_date, first_reply_date)/60.0 hours_to_first_reply
    from ac_yevgeniy.zendesk_ticket_summary
)

-- find average and mediam times to first reply
select distinct
    ticket_week, 
    avg(hours_to_first_reply) over(partition by ticket_week) as avg_hours_to_first_reply,
    median(hours_to_first_reply) over(partition by ticket_week) as med_hours_to_first_reply
from reply_times
order by ticket_week