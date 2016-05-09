with
ticket_info as
(
	select ticket_id, ticket_date
	from {{ref("zendesk_tickets")}}
),

audit_info as
(
	select *
	from {{ref("zendesk_ticket_audit_info")}}
),

admins_and_agents as
(
    select distinct user_id
    from {{ref("zendesk_users")}}
    where role in ('admin', 'agent')
),

first_audit_dates as
(
    select ticket_id, min(audit_date) as first_reply_date
    from audit_info
    where
        -- look at events from admins and agents only
        audit_author_id in (select * from admins_and_agents)
        -- look at comments only
        and audit_type in 'Comment'
        -- look at public audits only
        and is_audit_public = 1
    group by ticket_id, ticket_date
),

solved_dates as
(
	-- get the latest solved date if the ticket was reopened
    select ticket_id, max(audit_date) as solved_date
    from audit_info
    where audit_value = 'solved'
    group by ticket_id
)

select a.ticket_id, ticket_date, first_reply_date, solved_date
from ticket_info a
left outer join first_audit_dates b
	on a.ticket_id = b.ticket_id
left outer join solved_dates c
	on b.ticket_id = c.ticket_id

