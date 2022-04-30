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
      psi.completeddate
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
      'lastupdatedby',
      tei.lastupdatedby,
      'inactive',
      tei.inactive,
      'deleted',
      tei.deleted
    )
  ) as dt
from programstageinstance psi
  inner join organisationunit o using(organisationunitid)
  inner join programinstance pi using(programinstanceid)
  inner join trackedentityinstance tei using(trackedentityinstanceid);`;

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
  const { data } = await this.api.post(`wal/index?index=${index}`, {
    data: all,
  });
  console.log(data);
};

module.exports.batchSize = 1000;
