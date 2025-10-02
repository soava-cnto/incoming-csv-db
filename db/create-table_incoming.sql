CREATE TABLE call_logs (
    incoming_id UUID DEFAULT uuid_generate_v4(),
    semaine INT,
    datetime_appel TIMESTAMP,
    date_appel DATE,
    heure_appel TEXT,
    indice BIGINT,
    duree_prise_en_charge INT,
    duree_post_travail_agent INT,
    duree_appel INT,
    numero_telephone TEXT,
    numero_telephone_clean TEXT,
    id_agent_1 INT,
    id_agent_2 INT,
    nom_qualification TEXT,
    nom_qualification_detaillee TEXT,
    nom_agent TEXT,
    nom_campagne TEXT,
    sous_campagne TEXT,
    numero_court INT,
    raccrochage INT,
    commentaire TEXT
);


-- if error creating uuid 
-- use this before by activating uuid-ossp extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
