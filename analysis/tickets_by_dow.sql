with 
submission_day as 
(
	select
		ticket_date, extract('dow' from ticket_date) as dow_num,
		to_char(ticket_date, 'day') as dow
	from {{ref("zendesk_ticket_summary")}}
	where
		-- limit to this year's tickets
        ticket_date > '2016-01-01'
)

select dow, num_tickets
from
(
	select dow_num, dow, count(*) num_tickets
	from submission_day
	group by dow_num, dow
)
order by dow_num asc