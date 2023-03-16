DELETE FROM `{{ table_id }}` WHERE reference_date = '{{ reference_date }}';

INSERT INTO `{{ table_id }}`
(
    reference_date,
    corporate_registration_number,
    investor_name,
    is_invested_by_qii,
    is_invested_by_qoi
)
WITH RECENT_OCR AS (
    SELECT
        corporate_registration_number
        , MAX(reference_date) AS reference_date
    FROM `{{ project }}.gcs_shareholders.gcs_ocr_shareholders`
    GROUP BY
        corporate_registration_number
)
, OCR AS (
    SELECT
        DATE('{{ reference_date }}') AS reference_date
        , gos.corporate_registration_number
        , ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
            REPLACE(
                REPLACE(
                    UPPER(ocr_text)
                    , '(주)' , '')
                , 'ㆍ', '')
            , r'[\d\p{Letter}]'), ''
        ) AS ocr_text
    FROM `{{ project }}.gcs_shareholders.gcs_ocr_shareholders` AS gos, UNNEST(ocr_text) AS ocr_text
    INNER JOIN RECENT_OCR recent_ocr
    ON gos.reference_date = recent_ocr.reference_date
        AND gos.corporate_registration_number = recent_ocr.corporate_registration_number
    GROUP BY
        reference_date
       , gos.corporate_registration_number
       , ocr_text
)
, DICT AS (
    SELECT
        category
        , investor_main
        , ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(
            REPLACE(
                REPLACE(
                    UPPER(investor_deri)
                    , '(주)' , '')
                , 'ㆍ', '')
            , r'[\d\p{Letter}]'), ''
        ) AS investor_deri
        , exp_flag
    FROM `{{ project }}.tracker_dictionary.dic_investors`
)
, DICT_INCLUDE AS (
    SELECT
        category
        , investor_main
        , investor_deri
    FROM DICT
    WHERE exp_flag = 'Include'
)
, DICT_EXCLUDE AS (
    SELECT
        investor_deri
    FROM DICT
    WHERE exp_flag = 'Exclude'
    GROUP BY investor_deri
)
, CORP_EXCLUDE AS (
    SELECT
        ocr.corporate_registration_number
    FROM OCR ocr, DICT_EXCLUDE de
    WHERE INSTR(ocr.ocr_text, de.investor_deri) > 0
    GROUP BY ocr.corporate_registration_number
)
, OCR_EXCLUDE AS (
    SELECT
        ocr.reference_date
        , ocr.corporate_registration_number
        , ocr_text
        , ce.corporate_registration_number IS NOT NULL AS is_excluded
    FROM OCR ocr
    LEFT OUTER JOIN CORP_EXCLUDE ce
    ON ocr.corporate_registration_number = ce.corporate_registration_number
)

SELECT
    ocr.reference_date
    , ocr.corporate_registration_number
    , di.investor_main AS investor_name
    , LOGICAL_OR(di.category = 'QII' AND NOT ocr.is_excluded) AS is_invested_by_qii
    , LOGICAL_OR(di.category = 'QOI' AND NOT ocr.is_excluded) AS is_invested_by_qoi
FROM OCR_EXCLUDE ocr, DICT_INCLUDE di
WHERE INSTR(ocr.ocr_text, di.investor_deri) > 0
GROUP BY
    ocr.reference_date
    , ocr.corporate_registration_number
    , di.investor_main
