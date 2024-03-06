CREATE TABLE analytics
default character set utf8
default collate utf8_general_ci AS
WITH 
ticker_on AS (
    SELECT cnpj, tickeron AS ticker
    FROM fundamentei 
    WHERE cnpj <> 'na'
),
ticker_pn AS (
    SELECT cnpj, tickerpn AS ticker 
    FROM fundamentei 
    WHERE cnpj <> 'na'
),
tickers AS (
    SELECT * FROM ticker_on 
    UNION ALL 
    SELECT * FROM ticker_pn
),
atributos AS (
    SELECT 
        cad.CNPJ_CIA,
        cad.DENOM_COMERC,
        cad.DT_REG,
        cad.SIT,
        cad.AUDITOR,
        f.atuacao,
        f.segmento_listagem,
        f.tag_alongon,
        f.tag_alongpn,
        f.freefloaton,
        f.freefloatpn,
        f.on_percent,
        f.pn_percent,
        f.reclameaquinota,
        f.reclameaquitexto
    FROM cad
    LEFT JOIN fundamentei AS f 
    ON cad.cnpj_cia = f.cnpj
),

fundamentus AS (
    SELECT

        f.papel,
        f.cotacao,
        f.pl,
        f.pvp,
        f.psr,
        f.div_yield,
        f.p_ativo,
        f.p_cap_giro,
        f.p_ebit,
        f.p_ativ_circ_liq,
        f.ev_ebit,
        f.ev_ebitda,
        f.mrg_ebit,
        f.mrg_liq,
        f.roic,
        f.roe,
        f.liq_2meses,
        f.liq_corr,
        f.patrim_liq,
        f.div_bruta_patrim,
        f.cresc_rec_cinco_anos,
        t.*

    FROM fundamentus AS f
    LEFT JOIN tickers AS t ON f.Papel = t.ticker
    )

SELECT distinct * FROM fundamentus AS f

LEFT JOIN atributos AS a ON a.CNPJ_CIA = f.cnpj;


-- ALTER TABLE db.dfp_cia_aberta_DRE_con_2023 CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;

-- ALTER TABLE db.analytics CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;

-- SET @charset_collate = 'utf8mb4_unicode_ci'; -- Specify the desired character set and collation

-- SELECT CONCAT('ALTER TABLE `', TABLE_NAME, '` CONVERT TO CHARACTER SET utf8mb4 COLLATE ', @charset_collate, ';')
-- FROM information_schema.TABLES
-- WHERE TABLE_SCHEMA = 'db' AND TABLE_NAME = 'dfp_cia_aberta_DRE_con_2023';




