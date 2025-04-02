{{ config(
    materialized='view'
) }}

WITH departements AS (
    -- Départements métropolitains
    SELECT
        DEP AS dep,
        REG AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM {{ ref('staging__v_departement') }} 

    UNION

    -- Départements des DROM via v_comer
    SELECT
        COMER AS dep,
        COMER AS reg,
        NCC AS ncc,
        LIBELLE AS libelle
    FROM {{ ref('staging__v_comer') }}
)

SELECT * FROM departements