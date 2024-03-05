import os
import oracledb
from dotenv import dotenv_values
from datetime import datetime, timedelta, date
import pandas as pd
from mskpymail import send_email

__location__ = os.path.realpath(os.getcwd())
config = dotenv_values(__location__ + "/.env")

oracledb.init_oracle_client(lib_dir="C:/Oracle/instantclient_21_9/")
connection = oracledb.connect(
    user=config["DB_USER"], password=config["DB_PASS"], dsn=config["DB_NAME"]
)

yesterday = date.today() - timedelta(days=1)
five_days_ago = yesterday - timedelta(days=5)

from_date = datetime.strftime(five_days_ago, "%Y-%m-%d") + " 00:00:00"
to_date = datetime.strftime(yesterday, "%Y-%m-%d") + " 23:59:59"

parms = {":from_date": from_date, ":to_date": to_date}

print("from_date: ", from_date)
print("to_date: ", to_date)


genlab_query = """
SELECT
  per.NAME_FULL_FORMATTED as "person_name"
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) as "mrn"
  , v500.lab_fmt_accession(uces.accession_nbr) as "accession"
  , v500.omf_get_cv_display(uces.specimen_type_cd) as "specimen_type"
  , v500.omf_get_cv_display(uces.catalog_cd) as "order_procedure"
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126) as "date_time_order"
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) as "date_time_inlab"
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end as "age"
  , v500.omf_get_cv_display(per.sex_cd) as "sex"
  , v500.omf_get_cv_display(uces.patient_nu_cd) as "patient_location"
FROM PERSON PER
  , ORDERS O
  , UM_CHARGE_EVENT_ST UCES
WHERE (o.person_id = per.person_id)
AND (o.order_id = uces.order_id)
AND (uces.specimen_type_cd IN (1765.00,38245313.00,38245297.00,2552841975.00))
    AND (uces.in_lab_dt_tm BETWEEN cclsql_cnvtdatetimeutc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1) AND cclsql_cnvtdatetimeutc(to_date(:to_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1))
    AND (per.person_id NOT IN (13839881.00,13503455.00,13623964.00,13400335.00,13403030.00,13293788.00,13014783.00,13693940.00,12797578.00,13693938.00,13292471.00,13491890.00,13781699.00,13781700.00,13781697.00,13781702.00,13781701.00,13457375.00,13626156.00,13630673.00,13289617.00,13275702.00,13647653.00,13288771.00,13288746.00,13289794.00,12442193.00,12442199.00,13604550.00,13605205.00,12432296.00,13596905.00,13596913.00,13603369.00,13604866.00,13639091.00,13639107.00,13414244.00,13406645.00,13491337.00,13491561.00,13491889.00,13451165.00,13491891.00,13776441.00,13399729.00,13406404.00,13645072.00,13403979.00,13403982.00,13403983.00,13403984.00,13884685.00,13757957.00,13945236.00,12433918.00,12442052.00,12465533.00,13404845.00,13600598.00,13488166.00,13784952.00,13606039.00,13633353.00,13890464.00,13963625.00,13579486.00,13581513.00,13438645.00,13440757.00,12441992.00,13605212.00,13605386.00,12443245.00,13604571.00,13604563.00,13604570.00,13604572.00,13219303.00,13592908.00,13654007.00,13657253.00,13568031.00,12765049.00,13693942.00,12494157.00,13693941.00,13590552.00,13450385.00,13623190.00,12442594.00,13213386.00,13434579.00,13398416.00,13639105.00,13458208.00,13588468.00,13236295.00,13459702.00,13605388.00,13746151.00,13225106.00,13243418.00,13225109.00,13225113.00,13225103.00,13683624.00,13225102.00,13683623.00,13225112.00,13225107.00,13243422.00,13225094.00,13678770.00,13678772.00,13225105.00,13243413.00,13225104.00,13243408.00,13225108.00,13225111.00,13243461.00,13458355.00,13686040.00,13225101.00,13678773.00,13225110.00,13243424.00,13243456.00,13243441.00,13243439.00,13243459.00,13243425.00,13243454.00,13243447.00,13243436.00,13243433.00,13225100.00,13678771.00))
GROUP BY per.person_id, per.NAME_FULL_FORMATTED
  , per.PERSON_ID
  , uces.accession_nbr
  , uces.specimen_type_cd
  , v500.omf_get_cv_display(uces.specimen_type_cd)
  , uces.catalog_cd
  , v500.omf_get_cv_display(uces.catalog_cd)
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126)
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126)
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end
  , v500.omf_get_cv_display(per.sex_cd)
  , uces.patient_nu_cd
  , v500.omf_get_cv_display(uces.patient_nu_cd)
ORDER BY per.NAME_FULL_FORMATTED nulls first
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) nulls first
  , uces.accession_nbr nulls first
  , v500.omf_get_cv_display(uces.specimen_type_cd) nulls first
  , v500.omf_get_cv_display(uces.catalog_cd) nulls first
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126) nulls first
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) nulls first
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end nulls first
  , v500.omf_get_cv_display(per.sex_cd) nulls first
  , v500.omf_get_cv_display(uces.patient_nu_cd) nulls first
  """
  
cys_glu_query = """
SELECT
  per.NAME_FULL_FORMATTED as "person_name"
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) as "mrn"
  , v500.lab_fmt_accession(uces.accession_nbr) as "accession"
  , v500.omf_get_cv_display(uces.specimen_type_cd) as "specimen_type"
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126) as "date_time_order"
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) as "date_time_inlab"
  , v500.omf_get_cv_display(uces.catalog_cd) as "order_procedure"
  , Case when uces.activity_type_cd in (select unique code_value from code_value where code_set=106 and cdf_meaning in ('MICROBIOLOGY')) then '-' else v500.omf_get_cv_display(uces.task_assay_cd) end as "discrete_assay"
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)

  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)

  when '2' then pr.result_value_alpha

  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '4' then

    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')

      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)

    when (pr.long_text_id > 0)

      then v500.lab_get_long_text_nortf(pr.long_text_id)

    else pr.result_value_alpha

    end

  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')

  when '7' then pr.ascii_text

  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)

  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')

end as "result"
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end as "age"
  , v500.omf_get_cv_display(per.sex_cd) as "sex"
  , v500.omf_get_cv_display(uces.patient_nu_cd) as "patient_location"
FROM PERSON PER
  , ORDERS O
  , UM_CHARGE_EVENT_ST UCES
  , PERFORM_RESULT PR
  , RESULT R
WHERE (uces.result_id = r.result_id)
AND (uces.patient_id = per.person_id)
AND (pr.result_id = r.result_id
AND (o.order_id = uces.order_id)
 and pr.result_status_cd in (select unique code_value from code_value where code_set = 1901 and cdf_meaning in ('VERIFIED','AUTOVERIFIED','CORRECTED')))
AND (((o.catalog_cd IN (2552834073.00,2552834081.00,2552834065.00,2552925577.00))
    AND (uces.order_dt_tm BETWEEN cclsql_cnvtdatetimeutc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1) AND cclsql_cnvtdatetimeutc(to_date(:to_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1))
    AND (per.person_id NOT IN (13839881.00,13503455.00,13623964.00,13400335.00,13403030.00,13293788.00,13014783.00,13693940.00,12797578.00,13693938.00,13292471.00,13491890.00,13781699.00,13781700.00,13781697.00,13781702.00,13781701.00,13457375.00,13626156.00,13630673.00,13289617.00,13275702.00,13647653.00,13288771.00,13288746.00,13289794.00,12442193.00,12442199.00,13604550.00,13605205.00,12432296.00,13596905.00,13596913.00,13603369.00,13604866.00,13639091.00,13639107.00,13414244.00,13406645.00,13491337.00,13491561.00,13491889.00,13451165.00,13491891.00,13776441.00,13399729.00,13406404.00,13645072.00,13403979.00,13403982.00,13403983.00,13403984.00,13884685.00,13757957.00,13945236.00,12433918.00,12442052.00,12465533.00,13404845.00,13600598.00,13488166.00,13784952.00,13606039.00,13633353.00,13890464.00,13963625.00,13579486.00,13581513.00,13438645.00,13440757.00,12441992.00,13605212.00,13605386.00,12443245.00,13604571.00,13604563.00,13604570.00,13604572.00,13219303.00,13592908.00,13654007.00,13657253.00,13568031.00,12765049.00,13693942.00,12494157.00,13693941.00,13590552.00,13450385.00,13623190.00,12442594.00,13213386.00,13434579.00,13398416.00,13639105.00,13458208.00,13588468.00,13236295.00,13459702.00,13605388.00,13746151.00,13225106.00,13243418.00,13225109.00,13225113.00,13225103.00,13683624.00,13225102.00,13683623.00,13225112.00,13225107.00,13243422.00,13225094.00,13678770.00,13678772.00,13225105.00,13243413.00,13225104.00,13243408.00,13225108.00,13225111.00,13243461.00,13458355.00,13686040.00,13225101.00,13678773.00,13225110.00,13243424.00,13243456.00,13243441.00,13243439.00,13243459.00,13243425.00,13243454.00,13243447.00,13243436.00,13243433.00,13225100.00,13678771.00)))
  AND (uces.patient_fac_cd IS NULL
    OR uces.patient_fac_cd IN (0,2552831699.00,2552819553.00,2552819557.00,2552819545.00,2552819541.00,2552923205.00,2552819561.00,2552819565.00,2552831651.00,2552923151.00,2554829303.00,2554829285.00,2552831657.00,2555416181.00,2555301415.00,2552923169.00,2552831663.00,2552923187.00,2555366977.00,2552923403.00,2552878003.00,2552923367.00,2552923223.00,2555371203.00,2552831645.00,2552831687.00,2552909717.00,2552831669.00,2552819549.00,2552831705.00,2555402539.00,2554942199.00,2555478755.00,2552831675.00,2552923349.00,2552909699.00,2552923295.00,2552909681.00,2552923259.00,2555196575.00,2552615531.00,2552651407.00,2555672665.00,2552923277.00,2552923241.00,2555279575.00,2552909645.00,2552909663.00,2552922971.00,2552922989.00,2552923025.00,2552923007.00,2552922935.00,2552923043.00,2552923061.00,2552922953.00,2552923079.00,2552923097.00,2552923115.00,2552923133.00,2552923313.00,2555909329.00,2553335905.00,2552831681.00,2552831639.00,2552923385.00,2552831711.00,2552923331.00,2556017625.00,2555475409.00,2552831693.00,2552925817.00,2555269075.00,2560121111.00,2560682671.00,2561205975.00,2562996689.00)))
GROUP BY per.person_id, per.NAME_FULL_FORMATTED
  , per.PERSON_ID
  , uces.accession_nbr
  , uces.specimen_type_cd
  , v500.omf_get_cv_display(uces.specimen_type_cd)
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126)
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126)
  , uces.catalog_cd
  , v500.omf_get_cv_display(uces.catalog_cd)
  , uces.task_assay_cd
  , Case when uces.activity_type_cd in (select unique code_value from code_value where code_set=106 and cdf_meaning in ('MICROBIOLOGY')) then '-' else v500.omf_get_cv_display(uces.task_assay_cd) end
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)

  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)

  when '2' then pr.result_value_alpha

  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '4' then

    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')

      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)

    when (pr.long_text_id > 0)

      then v500.lab_get_long_text_nortf(pr.long_text_id)

    else pr.result_value_alpha

    end

  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')

  when '7' then pr.ascii_text

  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)

  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')

end
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end
  , v500.omf_get_cv_display(per.sex_cd)
  , uces.patient_nu_cd
  , v500.omf_get_cv_display(uces.patient_nu_cd)
ORDER BY per.NAME_FULL_FORMATTED nulls first
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) nulls first
  , uces.accession_nbr nulls first
  , v500.omf_get_cv_display(uces.specimen_type_cd) nulls first
  , cclsql_utc_cnvt(uces.order_dt_tm, 1,126) nulls first
  , cclsql_utc_cnvt(uces.in_lab_dt_tm, 1,126) nulls first
  , v500.omf_get_cv_display(uces.catalog_cd) nulls first
  , Case when uces.activity_type_cd in (select unique code_value from code_value where code_set=106 and cdf_meaning in ('MICROBIOLOGY')) then '-' else v500.omf_get_cv_display(uces.task_assay_cd) end nulls first
  , case v500.omf_get_cdf_meaning(pr.result_type_cd)

  when '1' then v500.lab_get_long_text_nortf(pr.long_text_id)

  when '2' then pr.result_value_alpha

  when '3' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '4' then

    case when (pr.long_text_id > 0 AND trim(pr.result_value_alpha) > ' ')

      then pr.result_value_alpha || ' - ' || v500.lab_get_long_text_nortf(pr.long_text_id)

    when (pr.long_text_id > 0)

      then v500.lab_get_long_text_nortf(pr.long_text_id)

    else pr.result_value_alpha

    end

  when '6' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd')

  when '7' then pr.ascii_text

  when '8' then trim(v500.lab_fmt_result(pr.service_resource_cd,r.task_assay_cd,pr.result_value_numeric,0))

  when '9' then v500.omf_get_cv_display(pr.result_code_set_cd)

  when '11' then to_char(cclsql_utc_cnvt(pr.result_value_dt_tm,1,126),'yyyy-mm-dd HH24:MI:SS')

end nulls first
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end nulls first
  , v500.omf_get_cv_display(per.sex_cd) nulls first
  , v500.omf_get_cv_display(uces.patient_nu_cd) nulls first
"""


rlclt_query = """
SELECT
  per.NAME_FULL_FORMATTED as "person_name"
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) as "mrn"
  , v500.lab_fmt_accession(uces.accession_nbr) as "accession"
  , v500.omf_get_cv_description(uces.specimen_type_cd) as "specimen_type"
   , v500.omf_get_cv_display(o.catalog_cd) as "order_procedure"
  , cclsql_utc_cnvt(uces.order_dt_tm , 1, 126) as "date_time_order"
  , cclsql_utc_cnvt(uces.in_lab_dt_tm , 1, 126) as "date_time_inlab"
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end as "age"
  , v500.omf_get_cv_display(per.sex_cd) as "sex"
    , v500.omf_get_cv_display(uces.patient_nu_cd) as "patient_location"
FROM PERSON PER
  , UM_CHARGE_EVENT_ST UCES
  , ORDERS O
WHERE (o.person_id = per.person_id)
AND (uces.order_id = o.order_id)
AND (uces.activity_type_cd = v500.omf_get_cvalue(106,'MICROBIOLOGY'))
AND (o.catalog_cd IN (2552839409.00))
  AND (uces.in_lab_dt_tm BETWEEN cclsql_cnvtdatetimeutc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1) AND cclsql_cnvtdatetimeutc(to_date(:to_date,'YYYY-MM-DD HH24:MI:SS'),1,126,1))
  AND (per.person_id NOT IN (13839881.00,13503455.00,13623964.00,13400335.00,13403030.00,13293788.00,13014783.00,13693940.00,12797578.00,13693938.00,13292471.00,13491890.00,13781699.00,13781700.00,13781697.00,13781702.00,13781701.00,13457375.00,13626156.00,13630673.00,13289617.00,13275702.00,13647653.00,13288771.00,13288746.00,13289794.00,12442193.00,12442199.00,13604550.00,13605205.00,12432296.00,13596905.00,13596913.00,13603369.00,13604866.00,13639091.00,13639107.00,13414244.00,13406645.00,13491337.00,13491561.00,13491889.00,13451165.00,13491891.00,13776441.00,13399729.00,13406404.00,13645072.00,13403979.00,13403982.00,13403983.00,13403984.00,13884685.00,13757957.00,13945236.00,12433918.00,12442052.00,12465533.00,13404845.00,13600598.00,13488166.00,13784952.00,13606039.00,13633353.00,13890464.00,13963625.00,13579486.00,13581513.00,13438645.00,13440757.00,12441992.00,13605212.00,13605386.00,12443245.00,13604571.00,13604563.00,13604570.00,13604572.00,13219303.00,13592908.00,13654007.00,13657253.00,13568031.00,12765049.00,13693942.00,12494157.00,13693941.00,13590552.00,13450385.00,13623190.00,12442594.00,13213386.00,13434579.00,13398416.00,13639105.00,13458208.00,13588468.00,13236295.00,13459702.00,13605388.00,13746151.00,13225106.00,13243418.00,13225109.00,13225113.00,13225103.00,13683624.00,13225102.00,13683623.00,13225112.00,13225107.00,13243422.00,13225094.00,13678770.00,13678772.00,13225105.00,13243413.00,13225104.00,13243408.00,13225108.00,13225111.00,13243461.00,13458355.00,13686040.00,13225101.00,13678773.00,13225110.00,13243424.00,13243456.00,13243441.00,13243439.00,13243459.00,13243425.00,13243454.00,13243447.00,13243436.00,13243433.00,13225100.00,13678771.00))
GROUP BY per.person_id, per.NAME_FULL_FORMATTED
  , per.PERSON_ID
  , v500.lab_fmt_accession(uces.accession_nbr) 
  , v500.omf_get_cv_description(uces.specimen_type_cd)
    , o.catalog_cd
  , cclsql_utc_cnvt(uces.order_dt_tm , 1, 126)
  , cclsql_utc_cnvt(uces.in_lab_dt_tm , 1, 126)
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end
  , v500.omf_get_cv_display(per.sex_cd)
   , v500.omf_get_cv_display(uces.patient_nu_cd)
ORDER BY per.NAME_FULL_FORMATTED nulls first
  , PM_GET_ALIAS('MRN', 0, per.PERSON_ID, 0, SYSDATE) nulls first
  , v500.lab_fmt_accession(uces.accession_nbr)  nulls first
  , v500.omf_get_cv_description(uces.specimen_type_cd) nulls first
    , v500.omf_get_cv_display(o.catalog_cd) nulls first
  , cclsql_utc_cnvt(uces.order_dt_tm , 1, 126) nulls first
  , cclsql_utc_cnvt(uces.in_lab_dt_tm , 1, 126) nulls first
  , case when round((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25) < 1 then round(cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm) else round ((cclsql_cnvtdatetimeutc(sysdate, 1, 145, 1) - per.birth_dt_tm)/365.25,0) end nulls first
  , v500.omf_get_cv_display(per.sex_cd) nulls first
   , v500.omf_get_cv_display(uces.patient_nu_cd) nulls first
"""

cur = connection.cursor()

genlab_result = cur.execute(genlab_query, parms).fetchall()
genlab_columns = [desc[0] for desc in cur.description]
genlab_df = pd.DataFrame(genlab_result, columns=genlab_columns)

cys_glu_result = cur.execute(cys_glu_query, parms).fetchall()
cys_glu_columns = [desc[0] for desc in cur.description]
cys_glu_df = pd.DataFrame(cys_glu_result, columns=cys_glu_columns)

rlclt_result = cur.execute(rlclt_query, parms).fetchall()
rlclt_columns = [desc[0] for desc in cur.description]
rlclt_df = pd.DataFrame(rlclt_result, columns=rlclt_columns)

cur.close()
connection.close()

df = pd.concat([genlab_df, cys_glu_df, rlclt_df], ignore_index=True)

mrns_with_blood = df[df['specimen_type'] == 'Blood']['mrn'].unique()
mrns_not_blood = df[df['specimen_type'] != 'Blood']['mrn'].unique()
filtered_df = df[df['mrn'].isin(mrns_with_blood) & df['mrn'].isin(mrns_not_blood)].sort_values(by='person_name').fillna('')

numeric_datetime = datetime.now().strftime("%Y_%m_%d")



filtered_df.to_csv(
    f"C:/QMS/CYSPCR Enroll/data/cyspcr Enroll {numeric_datetime}.csv",
    index=False,
)
attachment = f"C:/QMS/CYSPCR Enroll/data/cyspcr Enroll {numeric_datetime}.csv"
body = f"<h2>Patient's with lower respiratory and plasma specimens {five_days_ago} to {yesterday}</h2> <br> <h2>Good Day! Here is the Data you requested!</h2> <br>"

send_email(
    to=[
        "clarkb@mskcc.org",
        "leec14@mskcc.org",
        "asowato1@mskcc.org"
    ],
    subject=f"Patient's with lower respiratory and plasma specimens {five_days_ago} to {yesterday}",
    body=body,
    attachments=[attachment],
    username=config["AD_USERNAME"],
    password=config["AD_PASSWORD"],
)
