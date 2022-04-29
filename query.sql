select
  concat(tei.uid,psi.uid) as id,
  (
    select jsonb_object_agg(tea.uid, value) AS months
    from trackedentityattributevalue teav
      inner join trackedentityattribute tea using(trackedentityattributeid)
    where teav.trackedentityinstanceid = tei.trackedentityinstanceid
  ) || eventdatavalues || jsonb_build_object (
    'pi',
    jsonb_build_object(
      'created',
      pi.created,
      'lastupdated',
      pi.lastupdated,
      'createdatclient',
      pi.createdatclient,
      'lastupdatedatclient',
      pi.lastupdatedatclient,
      'incidentdate',
      pi.incidentdate,
      'enrollmentdate',
      pi.enrollmentdate,
      'enddate',
      pi.enddate,
      'followup',
      pi.followup,
      'completedby',
      pi.completedby,
      'deleted',
      pi.deleted,
      'storedby',
      pi.storedby,
      'status',
      pi.status
    )
  ) || jsonb_build_object(
    'event',
    jsonb_build_object(
      'uid',
      psi.uid,
      'code',
      psi.code,
      'created',
      psi.created,
      'lastupdated',
      psi.lastupdated,
      'createdatclient',
      psi.createdatclient,
      'lastupdatedatclient',
      psi.lastupdatedatclient,
      'lastsynchronized',
      psi.lastsynchronized,
      'programinstanceid',
      psi.programinstanceid,
      'programstageid',
      psi.programstageid,
      'deleted',
      psi.deleted,
      'storedby',
      psi.storedby,
      'duedate',
      psi.duedate,
      'executiondate',
      psi.executiondate,
      'organisationunitid',
      psi.organisationunitid,
      'status',
      psi.status,
      'completedby',
      psi.completedby,
      'completeddate',
      psi.completeddate
    )
  ) || jsonb_build_object(
    'tei',
    jsonb_build_object(
      'uid',
      tei.uid,
      'code',
      tei.code,
      'created',
      tei.created,
      'lastupdated',
      tei.lastupdated,
      'lastupdatedby',
      tei.lastupdatedby,
      'createdatclient',
      tei.createdatclient,
      'lastupdatedatclient',
      tei.lastupdatedatclient,
      'inactive',
      tei.inactive,
      'deleted',
      tei.deleted,
      'lastsynchronized',
      tei.lastsynchronized
    )
  ) as dt
from programstageinstance psi
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid);