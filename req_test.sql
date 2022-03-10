DROP TABLE IF EXISTS aps_count2
;
CREATE TEMPORARY TABLE aps_count2 ON COMMIT PRESERVE ROWS AS 
SELECT p.policyNumber
FROM haven_uw.policy p
	JOIN haven_uw.aps_order aps ON aps.policyId = p."_id"
--WHERE p.policyNumber = '400040479'
GROUP BY 1
HAVING COUNT(DISTINCT aps.orderId) = 1
;

DROP TABLE IF EXISTS aps_r2
;
CREATE TEMPORARY TABLE aps_r2 ON COMMIT PRESERVE ROWS AS 
SELECT DISTINCT p.policyNumber
, r."_id" 
, r.createdTime as requirement_start
, r.fulfilledTime as requirement_end
, RANK() OVER (PARTITION BY p.policyNumber ORDER BY r.createdTime) as req_rank
FROM haven_uw.policy p
   JOIN haven_uw.workflow w on p.application_id = w."referenceId"
   JOIN haven_uw.stage s ON w._id = s.workflow_id
   JOIN haven_uw.requirement r ON r.stage_id = s._id
WHERE REGEXP_SUBSTR((MAPLOOKUP(MapJSONExtractor(r.requirementDef), 'name')), '(\w+)\s+(\w+)\s+(\w+)', 1, 1, '', 1) = 'MU2R1' 
	AND r.fulfilled = TRUE 
ORDER BY p.policyNumber, r.createdTime
;

DROP TABLE IF EXISTS aps_orderer2
;
CREATE TEMPORARY TABLE aps_orderer2 ON COMMIT PRESERVE ROWS AS 
SELECT DISTINCT mp.policyNumber
, cah.userId 
, cah.createdTime as cah_start
FROM haven_analytics.main_policy mp
	JOIN haven_uw.case_action_history cah ON mp.uw_uw_policy_id = cah.uw_policy_id
WHERE cah.action = 'APS Ordered'
	AND cah.userId <> 'System'
;

DROP TABLE IF EXISTS aps_base2
;
CREATE TEMPORARY TABLE aps_base2 ON COMMIT PRESERVE ROWS AS 
SELECT DISTINCT 
mp.policyNumber
, cah.associatedId
, cah.createdTime
, ao.status 
, ao.physicianName
, ar.requirement_end
, o.userId
FROM haven_analytics.main_policy mp
	JOIN haven_uw.case_action_history cah ON mp.uw_uw_policy_id = cah.uw_policy_id
	LEFT JOIN haven_uw.aps_order ao ON ao.orderId = cah.associatedId
	LEFT JOIN aps_r ar ON ar.policyNumber = mp.policyNumber AND cah.createdTime BETWEEN TIMESTAMPADD(minute, -60, ar.requirement_start) AND TIMESTAMPADD(minute, 60, ar.requirement_start)
	LEFT JOIN aps_orderer o ON o.policyNumber = mp.policyNumber AND cah.createdTime BETWEEN TIMESTAMPADD(minute, -60, o.cah_start) AND TIMESTAMPADD(minute, 60, o.cah_start)
WHERE cah.action = 'APS Ordered'
	AND cah.userId = 'System'
	AND mp.channel = 'CAS'
;

DROP TABLE IF EXISTS pd2
;
CREATE TEMPORARY TABLE pd2 ON COMMIT PRESERVE ROWS AS 
SELECT DISTINCT p.policyNumber, pd.createdTime as docrecdate
FROM haven_uw.policy p
	LEFT JOIN haven_uw.policy_doc pd ON pd.policyId = p."_id"
WHERE pd.docType = 'APS'
	AND p.policyNumber IN (SELECT DISTINCT p.policyNumber
FROM haven_uw.policy p
	LEFT JOIN haven_uw.policy_doc pd ON pd.policyId = p."_id"
WHERE pd.docType = 'APS'
GROUP BY 1
HAVING COUNT(DISTINCT pd."_id") = 1)
;

DROP TABLE IF EXISTS requirement_aps2
;
CREATE TEMPORARY TABLE requirement_aps2 ON COMMIT PRESERVE ROWS AS 
SELECT x.*
FROM (
SELECT DISTINCT policyNumber
, 'APS' as name
, associatedId AS UniqueID
, createdTime as StatusDateTime
, 'Underwriting' as type
, 'Ordered' as status
, userId as statusbyId
, physicianName as doctorName
FROM aps_base2
UNION
SELECT DISTINCT policyNumber
, 'APS' as name
, associatedId AS UniqueID
, createdTime as StatusDateTime
, 'Underwriting' as type
, 'Cancelled' as status
, userId as statusbyId
, physicianName as doctorName
FROM aps_base2
WHERE status = 'canceled'
UNION
SELECT DISTINCT aps_base.policyNumber
, 'APS' as name
, associatedId AS UniqueID
, NVL(pd.docrecdate, requirement_end) as StatusDateTime
, 'Underwriting' as type
, 'Received' as status
, 'System' as statusbyId
, physicianName as doctorName
FROM aps_base2
	LEFT JOIN aps_count ON aps_base.policyNumber = aps_count.policyNumber
	LEFT JOIN pd ON aps_base.policyNumber = pd.policyNumber AND aps_count.policyNumber = aps_base.policyNumber 
WHERE status = 'complete'
	) x
	--LEFT JOIN pd ON x.policyNumber = pd.policyNumber 
--WHERE x.policyNumber = '402020506'
;

DROP TABLE IF EXISTS followup_base2
;
CREATE TEMPORARY TABLE followup_base2 ON COMMIT PRESERVE ROWS AS 
SELECT DISTINCT mp.policyNumber
, fu._id
, fu.status
, fu.createdDate
, fu.closedDate
, UW.massMutualId 
--, fu."type" 
--, qa.section
--, qa.question
--, qa.uwQuestion
, CASE
	WHEN NOT REGEXP_LIKE(qa.section, 'FU|CQ', 'i') Then 'SHQ'
	WHEN REGEXP_LIKE(COALESCE(qa.question, qa.uwQuestion), 'Questionnaire') THEN 'Questionaire'
	ELSE CASE WHEN fu."type" = 'customer' THEN 'UW' ELSE 'Issue' END || ' ' || 'Follow-up'
	END AS followUpType
FROM haven_analytics.main_policy mp 
	JOIN haven_uw.follow_up fu ON fu.policy_id = mp.uw_policy_id 
	LEFT JOIN haven_uw.follow_up_comment fuc on fuc.follow_up_id = fu._id
	LEFT JOIN haven_uw.follow_up_qa qa ON qa.follow_up_id = fu._id
	LEFT JOIN haven_uw.admin UW on UW._id = fu.adminId
WHERE mp.channel = 'CAS'
;

DROP TABLE IF EXISTS requirement_followup2
;
CREATE TEMPORARY TABLE requirement_followup2 ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT followup_base.policyNumber
, followupType as name
, _id AS UniqueID
, createdDate as StatusDateTime
, 'Underwriting' as type
, 'Opened' as status
, massMutualId as statusbyId
, NULL as doctorName
FROM followup_base2
WHERE followUpType IN ('UW Follow-up','Issue Follow-up','SHQ')
UNION
SELECT DISTINCT followup_base.policyNumber
, followupType as name
, _id AS UniqueID
, closedDate as StatusDateTime
, 'Underwriting' as type
, status
, massMutualId as statusbyId
, NULL as doctorName
FROM followup_base2
WHERE followUpType IN ('UW Follow-up','Issue Follow-up','SHQ')
	AND closedDate IS NOT NULL
	AND status IN ('Cancelled','Closed')
;

DROP TABLE IF EXISTS endpoint2
;
CREATE TEMPORARY TABLE endpoint2 ON COMMIT PRESERVE ROWS AS
--endPoint
SELECT DISTINCT 
pol.policyNumber
, CASE 
	WHEN e.endPointname IN ('GetMvrReport','ReceivedMvrReport') THEN 'MVR' 
	WHEN e.endPointname IN ('SendMibRequest','SendMillimanRequest') THEN 'MIB/Rx'
	WHEN e.endPointname IN ('SendMibRequest','SendMillimanRequest','RetrieveMillimanData') THEN 'MIB'
	WHEN e.endpointName IN ('NotifiedParamedScheduled') THEN 'Labs'
	WHEN e.endpointName IN ('RetrieveDHPData','SendDHPRequest') THEN 'DHP Data'
	ELSE e.endpointName 
	END as name
, NULL AS UniqueID
, d.endTime as StatusDateTime
, 'Underwriting' as type
, CASE 
	WHEN e.endpointName  IN ('GetMvrReport','SendMibRequest','SendMillimanRequest','NotifiedParamedScheduled','SendDHPRequest') THEN 'Ordered'
	WHEN e.endpointName IN ('ReceivedMvrReport','RetrieveMillimanData','RetrieveDHPData') THEN 'Received'
	END as status
, 'System' as statusbyId
, NULL as doctorName
from haven_uw.policy pol 
	join haven_uw.workflow a on pol.application_id = a.referenceId 
	join haven_uw.stage b on a._id = b.workflow_id 
	JOIN haven_uw.requirement r ON r.stage_id = b._id
	join haven_uw.activity c on b._id = c.stage_id 
	LEFT join haven_uw.execution d on c._id = d.activity_id 
	LEFT join haven_uw.interaction e on d.interactionId = e."_id"
WHERE  --pol.policyNumber = '400059949'AND 
e.endpointName IN ('GetMvrReport','SendMibRequest','SendMillimanRequest','NotifiedParamedScheduled','SendDHPRequest','ReceivedMvrReport','RetrieveMillimanData','RetrieveDHPData')
;

DROP TABLE IF EXISTS rx
;
CREATE TEMPORARY TABLE rx ON COMMIT PRESERVE ROWS AS
--Rx Received Code
SELECT DISTINCT
p.policyNumber
, 'Rx' as name
, rx.resultsURL AS UniqueID
, MAX(rx.createdTime) OVER (PARTITION BY rx.resultsURL) as StatusDateTime
, 'Underwriting' as type
, 'Received' as status
, 'System' as statusbyId
, NULL as doctorName
 FROM haven_uw.policy p
	JOIN haven_uw.rx_data rx on p._id = rx.policy_id
	JOIN haven_uw.rx_data_source rds on rds.rx_data_id = rx._id
--WHERE p.policyNumber = '400059949'
;

DROP TABLE IF EXISTS mib2
;
CREATE TEMPORARY TABLE mib2 ON COMMIT PRESERVE ROWS AS
-- MIB Received Code
SELECT DISTINCT
policy.policyNumber
, 'MIB' as name
, mib_data."_id" AS UniqueID
, MAX(mib_data.createdTime) OVER (PARTITION BY mib_data."_id") as StatusDateTime
, 'Underwriting' as type
, 'Received' as status
, 'System' as statusbyId
, NULL as doctorName
FROM haven_uw.policy policy
    JOIN haven_uw.mib_data mib_data ON policy._id = mib_data.policy_id
    LEFT JOIN haven_uw.mib_code mib_code ON mib_data._id = mib_code.mib_data_id
    LEFT JOIN haven_uw.mib_insurance_activity mib_insurance_activity ON mib_data._id = mib_insurance_activity.mib_data_id
    LEFT JOIN haven_uw.mib_person mib_person ON mib_insurance_activity.mib_person_id = mib_person._id
WHERE mib_code."source" = 'MIB'
	--AND policy.policyNumber = '400059949'
;

DROP TABLE IF EXISTS docs
;
CREATE TEMPORARY TABLE docs ON COMMIT PRESERVE ROWS AS
--MVR/Lab Docs Received Code
SELECT DISTINCT
p.policyNumber
, CASE WHEN pd.name IN ('MVR Report') THEN 'MVR'
	WHEN pd.name IN ('Lab Slip','Lab Results') THEN pd.name 
	END as name
, pd."_id" AS UniqueID
, pd."date" as StatusDateTime
, 'Underwriting' as type
, 'Received' as status
, 'System' as statusbyId
, NULL as doctorName
FROM haven_uw.policy p
	LEFT JOIN haven_uw.policy_doc pd ON pd.policyId = p."_id"
WHERE pd.name IN ('MVR Report','Lab Slip','Lab Results')
--AND p.policyNumber = '400059949'
;

DROP TABLE IF EXISTS labs2
;
CREATE TEMPORARY TABLE labs2 ON COMMIT PRESERVE ROWS AS
--Labs Recieved 
SELECT DISTINCT
p.policyNumber
, CASE 
	WHEN ltr."_template" = 'MedicalExamResult' AND ltr.testResultValue IS NOT NULL THEN 'Lab Results - Physical Measurements'
	WHEN ltr."_template" = 'LabTestResult' AND ltr.testResultValue IS NOT NULL AND ltr.testCode IN ('561','1003800760') THEN 'Lab Results - Oral Fluids'
	WHEN (ltr."_template" = 'MedicalExamResult' AND ltr.testResultValue IS NOT NULL AND ltr.testCode IN ('UrineTemperature'))
		OR (ltr.description LIKE '%URN%' AND ltr.testResultValue IS NOT NULL)
		THEN 'Lab Results - Urinalysis'
	WHEN REGEXP_LIKE(ltr.description,'GAMMA GLUTAMYLTRANSFERASE|SGOT (AST)|SGPT (ALT)|SGPT (ALT)') AND ltr.testResultValue IS NOT NULL
		THEN 'Lab Results - Blood Profile'
	END as name
, lr."_id" AS UniqueID
, ltr.createdTime as StatusDateTime
, 'Underwriting' as type
, 'Received' as status
, 'System' as statusbyId
, NULL as doctorName
FROM haven_uw.policy p 
    LEFT JOIN haven_uw.lab_records lr ON p.labRecords_id = lr._id 
    LEFT JOIN haven_uw.lab_test_result ltr ON lr._id = ltr.lab_records_id 
--WHERE p.policyNumber = '400033669'
;

/*
DROP TABLE IF EXISTS haven_analytics.req_test
;
CREATE TABLE haven_analytics.req_test AS
*/
SELECT *
FROM (
SELECT *
FROM requirement_aps 
UNION
SELECT *
FROM requirement_followup
UNION
SELECT *
FROM endpoint
UNION
SELECT *
FROM rx
UNION
SELECT *
FROM mib2
UNION
SELECT *
FROM docs
UNION
SELECT *
FROM labs2
)x 
ORDER BY policyNumber DESC, statusDateTime ASC
;
