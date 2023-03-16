SELECT
  cii.idxCorp,
  c.resCompanyNm,
  resAccountTrDate,
   rah.resAccountDesc1, --(거래구분/메모)
    rah.resAccountDesc2, --(적요)
    rah.resAccountDesc3, --(거래점)
    rah.resAccountDesc4,
    rah.resaccount_final,
    -- case when REGEXP_CONTAINS(rah.resaccount_final, :pg_dic_in_str) 
    --  AND NOT REGEXP_CONTAINS(rah.resaccount_final,:pg_dic_ex_str) then '01.pg_매출' else '02.normal_매출' end as sale_flag,
    rah.resAccountIn
FROM
  (SELECT resAccount, --(보낸분/받는분)
        resAccountDesc1, --(거래구분/메모)
        resAccountDesc2, --(적요)
        resAccountDesc3, --(거래점)
        resAccountDesc4,
        replace(concat(resAccountDesc2,resAccountDesc3),' ','') as resaccount_final,
        resAccountCurrency,
        resAccountTrDate
        , resAccountIn as resAccountIn 
        , resAccountOut as resAccountOut
  from `gowid-prd.ods_mysql_gowid.ResAccountHistory_*`
  where SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ) rah
  LEFT JOIN (
    SELECT
      raa.resAccount,
      raa.connectedId,
      raa.resAccountCurrency
    FROM
      `gowid-prd.ods_mysql_gowid.ResAccount_*` raa
      where SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  ) ra ON ra.resAccount = rah.resAccount AND ra.resAccountCurrency = rah.resAccountCurrency
  LEFT JOIN (
    SELECT
      idxCorp,
      connectedId
    FROM
   `gowid-prd.ods_mysql_gowid.ConnectedMng_*`
   where SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  ) cm ON cm.connectedId = ra.connectedId
  LEFT JOIN (
    SELECT
      idxCorp,
      issuanceStatus
    FROM
     `gowid-prd.ods_mysql_gowid.CardIssuanceInfo_*`
    WHERE SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      and cardType IN ('GOWID')
  ) cii ON cii.idxCorp = cm.idxCorp
  AND cii.issuanceStatus IN ('ISSUED', 'APPLY')
  LEFT JOIN (
    SELECT
      idx,
      resCompanyNm
    FROM
      `gowid-prd.ods_mysql_gowid.Corp_*`
    WHERE SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  ) c ON c.idx = cii.idxCorp
WHERE
  cii.idxCorp IS NOT NULL
  AND resCompanyNm IS NOT NULL
  AND safe_cast(rah.resAccountIn as int64) > 0
GROUP BY
        cii.idxCorp,
        c.resCompanyNm,
        resAccountTrDate,
        rah.resAccountDesc1, 
            rah.resAccountDesc2, 
            rah.resAccountDesc3, 
            rah.resAccountDesc4,
            rah.resaccount_final,
        -- case when REGEXP_CONTAINS(rah.resaccount_final, ":dic_pg_in") 
        -- AND NOT REGEXP_CONTAINS(rah.resaccount_final,":dic_pg_ex") then '01.pg_매출' else '02.normal_매출' end,
            rah.resAccountIn
ORDER BY
  idxCorp, resAccountTrDate desc