with
ticket_audit_info as
(
	select *
	from {{ref("zendesk_ticket_audit_info")}}
	where
		-- limit to this year's tickets
        ticket_date > '2016-01-01'
),

all_ticket_agents as
(
	select distinct ticket_id, audit_author_id
	from ticket_audit_info a
	inner join {{ref("zendesk_users")}} b
		on a.audit_author_id = b.user_id
	where role in ('admin', 'agent')
),

single_agent_tickets as
(
	select tickets.ticket_id, audit_author_id
	from
	(
		select ticket_id
		from
		(
			select ticket_id, count(distinct audit_author_id) as num_agents
			from all_ticket_agents
			group by ticket_id
		)
		where num_agents = 1
	) tickets
	inner join all_ticket_agents agents
	on tickets.ticket_id = agents.ticket_id
),


author_stats as
(
	select
		a.ticket_id, audit_author_id,
		datediff(min, ticket_date, solved_date)/60.0 hours_to_solved,
		satisfaction_score
	from single_agent_tickets a
	inner join {{ref("zendesk_ticket_summary")}} b
		on a.ticket_id = b.ticket_id
	-- limit to solved tickets only
	where solved_date is not null
)

select
	audit_author_id,
	count(ticket_id) as num_tickets_solved,
	avg(hours_to_solved) avg_hours_to_solved,
	avg(satisfaction_score) as avg_satisfaction_score
from author_stats
group by audit_author_id

