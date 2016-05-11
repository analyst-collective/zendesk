/*
	Data for some tickets is missing. Specifically, some tickets don't
	have a new event in the audit_event table
*/
with valid_tickets as
(
	select distinct ticket_id
	from zendesk.audits a
	inner join zendesk.audits__events e
		on a.id = e._rjm_source_key_id
	where e.value = 'new'
)

select distinct
	id as ticket_id,
	status,
	satisfaction_rating__score as satisfaction_rating,
	created_at::datetime as ticket_date
from zendesk.tickets
where id in (select * from valid_tickets)