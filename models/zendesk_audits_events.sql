select
	id as event_id,
	type as audit_type,
	public as is_audit_public,
	author_id,
	_rjm_source_key_id as audit_id,
	value
from zendesk.audits__events
where
	-- data for some tickets is missing. See zendesk_audits
	_rjm_source_key_id in
	(select audit_id from {{ref("zendesk_audits")}})
