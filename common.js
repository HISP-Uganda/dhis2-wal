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

module.exports.updateQuery = `update programstageinstance set executiondate = created where executiondate < '2021-03-10' or executiondate > CURRENT_DATE + interval '1 day';`;

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
where ps.uid = 'a1jCssI2LkW';`;

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
  and (
    tei.created >= LOCALTIMESTAMP - INTERVAL '5 minutes'
    or tei.lastupdated >= LOCALTIMESTAMP - INTERVAL '5 minutes'
    or psi.created >= LOCALTIMESTAMP - INTERVAL '5 minutes'
    or psi.lastupdated >= LOCALTIMESTAMP - INTERVAL '5 minutes'
  );`;

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
        const { data } = await this.api.post(`wal/bulk?index=${index}`, {
            data: all,
        });
        console.log(data.inserted);
        data.errorDocuments.forEach(({ error, document }) =>
            console.error(error, document)
        );
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
where ps.uid = 'a1jCssI2LkW' and psi.created >= '${start}' and psi.created < '${end}';`;
};

module.exports.queryByNIN = (nin) => {
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
where ps.uid = 'a1jCssI2LkW' tei.trackedentityinstanceid = (select trackedentityinstanceid from trackedentityattributevalue where value = '${nin}');`;
};

module.exports.processAndInsert2 = async (index, rows) => {
    try {
        const all = rows.map(
            ({ attributes, eventdatavalues, path, regpath, ...rest }) => {
                const processedEvents = _.fromPairs(
                    Object.entries(eventdatavalues).flatMap(
                        ([dataElement, value]) => {
                            return [
                                [dataElement, value.value],
                                [`${dataElement}_created`, value.created],
                                [
                                    `${dataElement}_last_updated`,
                                    value.lastUpdated,
                                ],
                                [
                                    `${dataElement}_created_by`,
                                    value.createdByUserInfo?.username,
                                ],
                                [
                                    `${dataElement}_last_updated_by`,
                                    value.lastUpdatedByUserInfo?.username,
                                ],
                            ];
                        }
                    )
                );
                rest = {
                    ...rest,
                    ...attributes,
                    ...processedEvents,
                    same_user: false,
                };
                if (path) {
                    const eventOrgUnit = _.fromPairs(
                        String(path)
                            .split("/")
                            .slice(1)
                            .map((x, i) => {
                                return [`event_level${i + 1}`, x];
                            })
                    );

                    rest = { ...rest, ...eventOrgUnit };
                }
                if (regpath) {
                    const registrationOrgUnit = _.fromPairs(
                        String(regpath)
                            .split("/")
                            .slice(1)
                            .map((x, i) => {
                                return [`reg_level${i + 1}`, x];
                            })
                    );
                    rest = { ...rest, ...registrationOrgUnit };
                }
                if (
                    rest.stage === "a1jCssI2LkW" &&
                    rest["LUIsbsm3okG"] &&
                    rest["bbnyNYD1wgS"] &&
                    rest["LUIsbsm3okG_created_by"] ===
                        rest["bbnyNYD1wgS_created_by"] &&
                    rest["LUIsbsm3okG_created"].slice(0, 10) ===
                        rest["bbnyNYD1wgS_created"].slice(0, 10)
                ) {
                    rest = { ...rest, same_user: true };
                }
                return rest;
            }
        );
        const { data } = await this.api.post(`wal/bulk?index=${index}`, {
            data: all,
        });
        console.log(data.inserted);
        data.errorDocuments.forEach(({ error, document }) =>
            console.error(error, document)
        );
    } catch (error) {
        console.log(error.message);
    }
};

module.exports.batchSize = 500;
