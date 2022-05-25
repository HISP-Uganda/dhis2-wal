select o.uid as orgUnit,
  o.path,
  ps.uid as stage,
  ps.name as stagename,
  (
    select ot.uid
    from organisationunit ot
    where ot.organisationunitid = tei.organisationunitid
  ) as regOrgUnit,
  (
    select ot.path
    from organisationunit ot
    where ot.organisationunitid = tei.organisationunitid
  ) as regPath,
  concat(tei.uid, psi.uid) as id,
  pi.created as "pi_created",
  pi.lastupdated as "pi_last_updated",
  pi.incidentdate as "pi_incident_date",
  pi.enrollmentdate as "pi_enrollment_date",
  pi.completedby as "pi_completed_by",
  pi.deleted as "pi_deleted",
  pi.storedby as "pi_stored_by",
  pi.status as "pi_status",
  psi.uid as "event_uid",
  psi.created as "event_created",
  psi.lastupdated as "event_last_updated",
  psi.deleted as "event_deleted",
  psi.storedby as "event_stored_by",
  psi.duedate as "event_duedate",
  psi.executiondate as "event_execution_date",
  psi.status as "event_status",
  psi.completedby as "event_completed_by",
  psi.completeddate as "event_completed_date",
  psi.createdbyuserinfo->>'username' as "event_created_by",
  psi.lastupdatedbyuserinfo->>'username' as "event_lastupdated_by",
  tei.uid as "tei_uid",
  tei.created as "tei_created",
  tei.lastupdated as "tei_last_updated",
  tei.inactive as "tei_inactive",
  tei.deleted as "tei_deleted",
  tei.storedby as "tei_stored_by",
  (
    select jsonb_object_agg(tea.uid, value) AS months
    from trackedentityattributevalue teav
      inner join trackedentityattribute tea using(trackedentityattributeid)
    where teav.trackedentityinstanceid = tei.trackedentityinstanceid
  ) as attributes,
  eventdatavalues
from programstageinstance psi
  inner join programstage ps using(programstageid)
  inner join organisationunit o using(organisationunitid)
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid)
where ps.uid = 'a1jCssI2LkW'
limit 10;



select o.uid as orgUnit,
  o.path,
  ps.uid as stage,
  ps.name as stagename,
  (
    select ot.uid
    from organisationunit ot
    where ot.organisationunitid = tei.organisationunitid
  ) as regOrgUnit,
  (
    select ot.path
    from organisationunit ot
    where ot.organisationunitid = tei.organisationunitid
  ) as regPath,
  concat(tei.uid, psi.uid) as id,
  pi.created as "pi_created",
  pi.lastupdated as "pi_last_updated",
  pi.incidentdate as "pi_incident_date",
  pi.enrollmentdate as "pi_enrollment_date",
  pi.completedby as "pi_completed_by",
  pi.deleted as "pi_deleted",
  pi.storedby as "pi_stored_by",
  pi.status as "pi_status",
  psi.uid as "event_uid",
  psi.created as "event_created",
  psi.lastupdated as "event_last_updated",
  psi.deleted as "event_deleted",
  psi.storedby as "event_stored_by",
  psi.duedate as "event_duedate",
  psi.executiondate as "event_execution_date",
  psi.status as "event_status",
  psi.completedby as "event_completed_by",
  psi.completeddate as "event_completed_date",
  psi.createdbyuserinfo->>'username' as "event_created_by",
  psi.lastupdatedbyuserinfo->>'username' as "event_lastupdated_by",
  tei.uid as "tei_uid",
  tei.created as "tei_created",
  tei.lastupdated as "tei_last_updated",
  tei.inactive as "tei_inactive",
  tei.deleted as "tei_deleted",
  tei.storedby as "tei_stored_by",
  (
    select jsonb_object_agg(tea.uid, value) AS months
    from trackedentityattributevalue teav
      inner join trackedentityattribute tea using(trackedentityattributeid)
    where teav.trackedentityinstanceid = tei.trackedentityinstanceid
  ) as attributes,
  eventdatavalues
from programstageinstance psi
  inner join programstage ps using(programstageid)
  inner join organisationunit o using(organisationunitid)
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid)
where ps.uid = 'a1jCssI2LkW' and tei.trackedentityinstanceid = (select trackedentityinstanceid from trackedentityattributevalue where value = 'CF590371022MVG' limit 1);