DROP TABLE IF EXISTS x
;
CREATE TEMPORARY TABLE x ON COMMIT PRESERVE ROWS AS
SELECT rr.policyNumber 
, MAX(CAST(dv.uwRuleValue AS FLOAT)) appliedForFaceAmount_rr
FROM haven_analytics.rule_result rr 
	JOIN haven_uw.uw_rule_result_desc_value dv USING (uw_rule_result_id) 
WHERE rr.gate IN ('0','1')
 AND rr.ruleName = 'face_amount' 
AND dv.uwRuleDesc = 'face_amount_2' GROUP BY 1
;


DROP TABLE IF EXISTS z
;
CREATE TEMPORARY TABLE z ON COMMIT PRESERVE ROWS AS
SELECT mp.policyNumber
, CASE 
	WHEN mp.jointAppType = 'single' THEN COALESCE(x.appliedForFaceAmount_rr,mp.faceAmount)
	WHEN MAX(x.appliedForFaceAmount_rr) OVER (PARTITION BY mp.application_id) IS NULL THEN mp.faceAmount 
	WHEN MAX(x.appliedForFaceAmount_rr) OVER (PARTITION BY mp.application_id) = SUM(mp.faceAmount) OVER (PARTITION BY mp.application_id) THEN mp.faceAmount
	ELSE NULL
	END as appliedForFaceAmount
, CASE 
	WHEN MAX(x.appliedForFaceAmount_rr) OVER (PARTITION BY mp.application_id)  IS NULL THEN SUM(mp.faceAmount) OVER (PARTITION BY mp.application_id) 
	ELSE  MAX(x.appliedForFaceAmount_rr) OVER (PARTITION BY mp.application_id)
	END as totalAppliedForFaceAmount
, x.appliedForFaceAmount_rr
, CAST(((DATEDIFF(DAY, ins.dob , mp.submittedAt))/(365.25)) AS DECIMAL (10,2)) as insuredAge
FROM haven_analytics.main_policy mp
	LEFT JOIN x ON mp.policyNumber = x.policyNumber
	LEFT JOIN haven.policy pol ON mp.policyNumber = pol.policyNumber 
    LEFT JOIN haven.applicant applicant ON applicant._id = pol.insured_id
    LEFT JOIN haven.party ins ON ins._id = applicant.party_id
;

DROP TABLE IF EXISTS ab
;
CREATE TEMPORARY TABLE ab ON COMMIT PRESERVE ROWS AS
SELECT mp.policyNumber
, z.appliedForFaceAmount_rr
, z.appliedForFaceAmount
, z.totalAppliedForFaceAmount
, mp.totalFaceAmount
, z.insuredAge 
, CASE 
	WHEN mp.product = 'HavenTerm' THEN 'Haven Term'
	WHEN mp.product IN ('HavenTermSI', 'HavenTermSIT') THEN 'Haven SI'
	WHEN mp.product IN ('CPL65','CPL100')
       AND mp.jointAppType = 'single'
       AND z.totalAppliedForFaceAmount <= 250000										
       AND mp.insuredActualAgeAtSubmit >= 17.5 AND mp.insuredActualAgeAtSubmit < 46
       AND DATE(mp.igoDate) BETWEEN DATE('05/19/2020') AND DATE('12/1/2020') THEN 'Coverpath Special WL Fluidless'
	WHEN z.totalAppliedForFaceAmount <= 250000										
       AND mp.insuredActualAgeAtSubmit >= 17 AND mp.insuredActualAgeAtSubmit < 51
   AND mp.channel = 'CAS'
       AND DATE(mp.igoDate) >= ('12/2/2020') THEN 'Coverpath Special Fluidless'
	WHEN mp.channel = 'CAS' 
   	   AND mp.product LIKE '%CPL%'          
       AND mp.insuredActualAgeAtSubmit < 17.5 THEN 'Coverpath Juvie'
	WHEN mp.channel = 'CAS'  THEN 'Coverpath Adult'
ELSE mp.channel
END AS uwPlatform
FROM haven_analytics.main_policy mp
	LEFT JOIN z ON mp.policyNumber = z.policyNumber
;
	   

/*DROP TABLE IF EXISTS haven_analytics_dev.cp_special_path_retention_temp
;
CREATE TABLE haven_analytics_dev.cp_special_path_retention_temp AS*/
SELECT DISTINCT 
mp.policyNumber,
mp.submittedAt,
papi.issuedAge,
ab.appliedForFaceAmount,
ab.appliedForFaceAmount_rr,
ab.totalAppliedForFaceAmount,
ab.insuredAge,
mp.totalFaceAmount,
mp.productType,
mp.term,
mp.issuedDate,
CASE 
	WHEN mp.finalUWType = 'Accelerated' THEN 'Instant'
	WHEN mp.finalUWType = 'LiteTouch' THEN 'LiteTouch'
	END AS finalUWType,
mp.finalRateClass,
painsured.lastName,
ab.uwPlatform,
CASE WHEN pr.type = 'RTR' THEN 20 ELSE 1 END AS coverage,
100 AS company
FROM haven_analytics.main_policy mp 
	LEFT JOIN haven_analytics.main_uw_policy mup ON mp.policyNumber = mup.policyNumber 
	LEFT JOIN ab ON mp.policyNumber = ab.policyNumber
	LEFT JOIN haven.policy p ON p.policyNumber = mp.policyNumber
	LEFT JOIN haven.policy_rider pr ON pr.policy_id = p._id 
	LEFT JOIN haven_pa.policy pap ON pap.policyNumber = mp.policyNumber
	LEFT JOIN haven_pa.policy_info papi ON papi.policy_id = pap._id 
	LEFT JOIN haven_pa.insured i ON i._id = pap.insured_id
	LEFT JOIN haven_pa.party painsured ON painsured._id = i.party_id
 WHERE ab.uwPlatform IN ('Coverpath Special Fluidless','Coverpath Special WL Fluidless')
	AND mp.issuedDate IS NOT NULL
	AND mp.finalUWType IN ('LiteTouch','Accelerated')
;
