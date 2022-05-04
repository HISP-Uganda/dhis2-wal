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
      'incidentdate',
      pi.incidentdate,
      'enrollmentdate',
      pi.enrollmentdate,
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
      'created',
      psi.created,
      'lastupdated',
      psi.lastupdated,
      'deleted',
      psi.deleted,
      'storedby',
      psi.storedby,
      'duedate',
      psi.duedate,
      'executiondate',
      psi.executiondate,
      'status',
      psi.status,
      'completedby',
      psi.completedby,
      'completeddate',
      psi.completeddate,
      'createdbyuserinfo',
      psi.createdbyuserinfo,
      'lastupdatedbyuserinfo',
      psi.lastupdatedbyuserinfo
    )
  ) || jsonb_build_object(
    'tei',
    jsonb_build_object(
      'uid',
      tei.uid,
      'created',
      tei.created,
      'lastupdated',
      tei.lastupdated,
      'inactive',
      tei.inactive,
      'deleted',
      tei.deleted,
      'storedby',
      tei.storedby
    )
  ) as dt
from programstageinstance psi
  inner join programstage ps using(programstageid)
  inner join organisationunit o using(organisationunitid)
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid) where psi.storedby = 'data.spt' limit 10;