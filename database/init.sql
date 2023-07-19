CREATE TABLE registro_brt (
    id                      SERIAL PRIMARY KEY,
    codigo                  VARCHAR(50) NOT NULL,
    placa                   VARCHAR(7),
    linha                   VARCHAR(10),
    latitude                DECIMAL NOT NULL,
    longitude               DECIMAL NOT NULL,
    datahora                TIMESTAMP,
    velocidade              DECIMAL,
    id_migracao_trajeto     INTEGER,
    sentido                 VARCHAR(10),
    trajeto                 VARCHAR(100),
    hodometro               DECIMAL,
    direcao                 VARCHAR(10),
    created_at              TIMESTAMP DEFAULT current_timestamp
);