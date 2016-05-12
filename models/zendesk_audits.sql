select
	id as audit_id,
    ticket_id,
    created_at::datetime as audit_date
from zendesk.audits
where
	-- data for some tickets is missing. See zendesk_tickets
	ticket_id in
	(select ticket_id from {{ref("zendesk_tickets")}})
