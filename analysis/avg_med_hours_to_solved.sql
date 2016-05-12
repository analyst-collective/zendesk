with 
solved_times as
(
    select
        ticket_id, date_trunc('week',ticket_date)::date as ticket_week,
        datediff(min, ticket_date, solved_date)/60.0 hours_to_solved
    from {{ref("zendesk_ticket_summary")}}
    where
        -- limit to this year's tickets that we solved
        ticket_date > '2016-01-01'
        and solved_date is not null
)

-- find average and median times to ticket solved
select distinct
    ticket_week, 
    avg(hours_to_solved) over(partition by ticket_week) as avg_hours_to_solved,
    median(hours_to_solved) over(partition by ticket_week) as med_hours_to_solved
from solved_times
order by ticket_week