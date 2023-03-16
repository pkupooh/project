SELECT
  cii.idxCorp,
  c.resCompanyNm,
  resAccountDesc1,
  resAccountDesc2,
  resAccountDesc3,
  resAccountDesc4,
  resaccount_final,
  resAccountTrDate,
   rah.resAccountIn,
   rah.resAccountOut
FROM
  (SELECT resAccount, --(보낸분/받는분)
  resAccountDesc1, --(거래구분/메모)
  resAccountDesc2, --(적요)
  resAccountDesc3, --(거래점)
  resAccountDesc4,
  replace(concat(resAccountDesc2,resAccountDesc3),' ','') as resaccount_final,
  resAccountCurrency,
  resAccountTrDate
  , FORMAT(resAccountIn,0) as resAccountIn 
  , FORMAT(resAccountOut,0) as resAccountOut
  from gowid.ResAccountHistory ) rah
  LEFT JOIN (
    SELECT
      raa.resAccount,
      raa.connectedId,
      raa.resAccountCurrency
    FROM
      gowid.ResAccount raa
  ) ra ON ra.resAccount = rah.resAccount AND ra.resAccountCurrency = rah.resAccountCurrency
  LEFT JOIN (
    SELECT
      idxCorp,
      connectedId
    FROM
   gowid.ConnectedMng
  ) cm ON cm.connectedId = ra.connectedId
  LEFT JOIN (
    SELECT
      idxCorp,
      issuanceStatus
    FROM
     gowid.CardIssuanceInfo
    WHERE
      cardType IN ('GOWID')
  ) cii ON cii.idxCorp = cm.idxCorp
  AND cii.issuanceStatus IN ('ISSUED', 'APPLY')
  LEFT JOIN (
    SELECT
      idx,
      resCompanyNm
    FROM
      gowid.Corp
  ) c ON c.idx = cii.idxCorp
WHERE
  cii.idxCorp IS NOT NULL
  AND resCompanyNm IS NOT NULL
  AND resAccountIn > 0
GROUP BY
  cii.idxCorp,
  c.resCompanyNm,
  resAccountDesc1,
  resAccountDesc2,
  resAccountDesc3,
  resAccountDesc4,
  resaccount_final,
  resAccountTrDate,
   rah.resAccountIn ,
   rah.resAccountOut
ORDER BY
  idxCorp, resAccountTrDate desc
  ;
