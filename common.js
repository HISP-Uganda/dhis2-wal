const axios = require("axios");

module.exports.api = axios.create({
  // baseURL: "http://localhost:3001/",
  baseURL: "https://services.dhis2.hispuganda.org/",
});

module.exports.query = `select
  o.uid as orgUnit,
  o.path,
  (select ot.uid from organisationunit ot where ot.organisationunitid = tei.organisationunitid) as regOrgUnit,
  (select ot.path from organisationunit ot where ot.organisationunitid = tei.organisationunitid) as regPath,  
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
  inner join organisationunit o using(organisationunitid)
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid);`;
