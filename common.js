const axios = require("axios");
const _ = require("lodash");

const hirarchy = {
  0: "national",
  1: "region",
  2: "district",
  3: "subcounty",
  4: "facility",
};

module.exports.api = axios.create({
  // baseURL: "http://localhost:3001/",
  baseURL: "https://services.dhis2.hispuganda.org/",
});

module.exports.backlogQuery = `select o.uid ou,
  o.name,
  o.path,
  psi.programstageinstanceid::text,
  psi.uid,
  to_char(psi.created, 'YYYY-MM-DD') created,
  to_char(psi.created, 'MM') m,
  to_char(psi.lastupdated, 'YYYY-MM-DD') lastupdated,
  programinstanceid::text,
  programstageid::text,
  attributeoptioncomboid::text,
  psi.deleted,
  psi.storedby,
  to_char(duedate, 'YYYY-MM-DD') duedate,
  to_char(executiondate, 'YYYY-MM-DD') executiondate,
  psi.organisationunitid::text,
  status,
  completedby,
  to_char(completeddate, 'YYYY-MM-DD') completeddate,
  eventdatavalues->'bbnyNYD1wgS'->>'value' as vaccine,
  eventdatavalues->'LUIsbsm3okG'->>'value' as dose,
  assigneduserid::text,
  psi.createdbyuserinfo,
  psi.lastupdatedbyuserinfo
from programstageinstance psi
  inner join organisationunit o using(organisationunitid)
where psi.created >= LOCALTIMESTAMP - INTERVAL '3 hours 5 minutes'
  and programstageid = 12715;`;

module.exports.query = `select o.uid as orgUnit,
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
  inner join trackedentityinstance tei using(trackedentityinstanceid) where ps.uid = 'a1jCssI2LkW';`;

module.exports.intervalQuery = `select o.uid as orgUnit,
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
  inner join trackedentityinstance tei using(trackedentityinstanceid)
where ps.uid = 'a1jCssI2LkW'
  and (
    tei.created >= LOCALTIMESTAMP - INTERVAL '3 hours 5 minutes'
    or tei.lastupdated >= LOCALTIMESTAMP - INTERVAL '3 hours 5 minutes'
    or psi.created >= LOCALTIMESTAMP - INTERVAL '3 hours 5 minutes'
    or psi.lastupdated >= LOCALTIMESTAMP - INTERVAL '3 hours 5 minutes'
  )`;

module.exports.monthlyBacklogQuery = (date) => `select o.uid as orgUnit,
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
  inner join trackedentityinstance tei using(trackedentityinstanceid)
where ps.uid = 'a1jCssI2LkW'
  and (
    to_char(tei.created, 'YYYY-MM') = '${date}'
    or to_char(tei.lastupdated, 'YYYY-MM') = '${date}'
    or to_char(psi.created, 'YYYY-MM') = '${date}'
    or to_char(psi.lastupdated, 'YYYY-MM') = '${date}'
  )`;

module.exports.processAndInsert = async (index, rows) => {
  const all = rows.map(({ path, ...others }) => {
    const units = _.fromPairs(
      String(path)
        .split("/")
        .slice(1)
        .map((x, i) => {
          return [hirarchy[i] || "other", x];
        })
    );
    return {
      ...others,
      path: units,
    };
  });
  try {
    const { data } = await this.api.post(`wal/index?index=${index}`, {
      data: all,
    });
    console.log(data);
  } catch (error) {
    console.log(error.message);
  }
};

module.exports.createBacklogQuery = (start, end) => {
  return `select o.uid as orgUnit,
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
  inner join trackedentityinstance tei using(trackedentityinstanceid) where ps.uid = 'a1jCssI2LkW' and psi.created >= '${start}' and psi.created < '${end}';`;
};

module.exports.processAndInsert2 = async (index, rows) => {
  const all = rows.map(
    ({ id, stage, stagename, dt, orgunit, path, regorgunit, regpath }) => {
      const eventOrgUnit = _.fromPairs(
        String(path)
          .split("/")
          .slice(1)
          .map((x, i) => {
            return [`level${i + 1}`, x];
          })
      );
      const registrationOrgUnit = _.fromPairs(
        String(regpath)
          .split("/")
          .slice(1)
          .map((x, i) => {
            return [`level${i + 1}`, x];
          })
      );
      let all = { id, ...dt, stage, stagename };
      if (dt && dt.event) {
        all = { ...all, event: { ...dt.event, ...eventOrgUnit, orgunit } };
      }

      if (dt && dt.tei) {
        all = {
          ...all,
          tei: { ...dt.tei, ...registrationOrgUnit, regorgunit },
        };
      }
      return all;
    }
  );
  const { data } = await this.api.post(`wal/index?index=${index}`, {
    data: all,
  });
  console.log(data);
};

module.exports.batchSize = 2500;
