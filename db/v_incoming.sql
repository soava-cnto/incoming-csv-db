DROP VIEW IF EXISTS v_incoming_reiteration;

CREATE VIEW v_incoming_reiteration AS
SELECT
    incoming_id,
    semaine,
    datetime_appel,
    date_appel,
    heure_appel,
    indice,
    duree_prise_en_charge,
    duree_post_travail_agent,
    duree_appel,
    numero_telephone,
    numero_telephone_clean,
    id_agent_1,
    id_agent_2,
    nom_qualification,
    nom_qualification_detaillee,
    nom_agent,
    nom_campagne,
    sous_campagne,
    numero_court,
    raccrochage,
    CAST(
        CONCAT(
            CAST(NOM_QUALIFICATION AS TEXT),
            '/',
            CAST(NOM_QUALIFICATION_DETAILLEE AS TEXT)
        ) AS TEXT
    ) AS Concat_Typo,
    (
        date_trunc('hour', heure_appel :: time) + (
            extract(
                minute
                from
                    heure_appel :: time
            ) :: int / 30
        ) * interval '30 minutes'
    ) AS tranche_30min,
    (date_trunc('hour', heure_appel :: time)) AS tranche_heure,
    -- reiteration heure
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (
            PARTITION BY numero_telephone,
            date_appel,
            EXTRACT(
                HOUR
                FROM
                    CAST(heure_appel AS TIME)
            )
        ) THEN 0
        ELSE 1
    END AS reit_heure,
    -- reiteration jour
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (PARTITION BY numero_telephone, date_appel) THEN 0
        ELSE 1
    END AS reit_jour,
    -- reiteration semaine
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (
            PARTITION BY numero_telephone,
            date_appel,
            semaine
        ) THEN 0
        ELSE 1
    END AS reit_semaine,
    -- reiteration par qualif heure
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (
            PARTITION BY numero_telephone,
            date_appel,
            EXTRACT(
                HOUR
                FROM
                    CAST(heure_appel AS TIME)
            ),
            nom_qualification,
            nom_qualification_detaillee
        ) THEN 0
        ELSE 1
    END AS reit_qualif_heure,
    -- reiteration par qualif jour
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (
            PARTITION BY numero_telephone,
            date_appel,
            nom_qualification,
            nom_qualification_detaillee
        ) THEN 0
        ELSE 1
    END AS reit_qualif_jour,
    -- reiteration par qualif semaine
    CASE
        WHEN datetime_appel = MAX(datetime_appel) OVER (
            PARTITION BY numero_telephone,
            date_appel,
            semaine,
            nom_qualification,
            nom_qualification_detaillee
        ) THEN 0
        ELSE 1
    END AS reit_qualif_semaine,
    -- 
    CASE
        WHEN indice IS NOT NULL THEN 1
    END AS recu,
    CASE
        WHEN id_agent_1 <> 0 THEN 1
        ELSE 0
    END AS traite,
    CASE
        WHEN id_agent_1 <> 0
        AND duree_prise_en_charge <= 20 THEN 1
        ELSE 0
    END AS traite_SL,
    CASE
        WHEN nom_qualification = 'transfert'
        OR nom_qualification = 'REROUTAGE' THEN 1
        ELSE 0
    END AS transfert,
    CASE
        WHEN duree_appel <= 10 THEN 1
        ELSE 0
    END AS appel_moins_10s,
    CASE
        WHEN duree_appel <= 15 THEN 1
        ELSE 0
    END AS appel_moins_15s,
    CASE
        WHEN duree_appel <= 50 THEN 1
        ELSE 0
    END AS appel_moins_50s
FROM
    public.incoming_logs
WHERE
    nom_campagne NOT LIKE '%CRCM%';