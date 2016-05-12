with 
submission_time as 
(
	select extract(hour from ticket_date) as ticket_hour
	from {{ref("zendesk_ticket_summary")}}
	where
		-- limit to this year's tickets
        ticket_date > '2016-01-01'
)

select *
from
(
	select ticket_hour, count(*) num_tickets
	from submission_time
	group by ticket_hour
)
order by ticket_hour asc

