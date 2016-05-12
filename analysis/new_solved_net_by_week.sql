with 
ticket_summary as 
(
	select
		*, date_trunc('week', ticket_date)::date as ticket_week,
		date_trunc('week', solved_date)::date as solved_week
	from {{ref("zendesk_ticket_summary")}}
	where
		-- limit to this year's tickets
        ticket_date > '2016-01-01'
),

new_tickets as
(
	select ticket_week as week, count(*) as num_new
	from ticket_summary
	group by ticket_week
),

solved_tickets as
(
	select solved_week as week, count(*) as num_solved
	from ticket_summary
	-- make sure the ticket is actually solved
	where solved_week is not null
	group by solved_week
)

select
	week, num_new, num_solved,
	sum(backlog) over(order by week rows unbounded preceding) as cum_backlog
from
(
	select
		new_tickets.week, nvl(num_new,0) as num_new, nvl(num_solved,0) as num_solved,
		(nvl(num_new,0) - nvl(num_solved,0)) as backlog
	from new_tickets
	left outer join solved_tickets
		on new_tickets.week = solved_tickets.week
	order by new_tickets.week
)
order by week

