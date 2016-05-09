with 
solved_times as
(
    select
        ticket_id, date_trunc('week',ticket_date)::date as ticket_week,
        datediff(min, ticket_date, solved_date)/60.0 hours_to_solved
    from ac_yevgeniy.zendesk_ticket_summary
)

-- find average and mediam times to first reply
select distinct
    ticket_week, 
    avg(hours_to_solved) over(partition by ticket_week) as avg_hours_to_solved,
    median(hours_to_solved) over(partition by ticket_week) as med_hours_to_solved
from solved_times
order by ticket_week