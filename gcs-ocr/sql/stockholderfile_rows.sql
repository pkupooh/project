SELECT
    `idx`
    , `createdAt`
    , `fname`
    , `s3Key`
FROM `gowid`.`StockholderFile`
WHERE `createdAt` >= '{{ from }}' AND `createdAt` < '{{ to }}'
  AND `idx` NOT IN (973, 974, 1000, 1001, 9853)
-- AND (UPPER(fname) LIKE '%.JPG' OR UPPER(fname) LIKE '%.JPEG' OR UPPER(fname) LIKE '%.PDF')
-- 973, 974, 1000, 1001 중복
-- 9853 이미지파일 문제
-- WHERE `createdAt` >= '2021-10-18' AND `createdAt` < '2021-10-19'
