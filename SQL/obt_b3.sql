-- #CREATE TABLE obt_b3 AS

-- #Descobrindo cnpjs duplicados
-- #Aqui foi descoberto que a tabela fundamentei tem linhas duplicadas
-- #E também que deve-se filtrar cad.SIT = 'ATIVO' AND cad.TP_MERC = 'BOLSA'
-- #cnpj_duplicado AS (
-- #	SELECT 
-- #		CNPJ_CIA AS cnpj_dup
-- #	FROM atributos
-- #		GROUP BY CNPJ_CIA
-- #			HAVING count(*) > 1
-- #)

-- #	SELECT 
		
-- #		cnpj_dup,
-- #		atributos.*		 
		
-- #	FROM cnpj_duplicado
-- #	LEFT JOIN atributos
-- #	ON cnpj_dup = CNPJ_CIA

WITH 
# início: Criando uma tabela com os tickers e seus cnpjs
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
    SELECT distinct * FROM ticker_on 
    UNION ALL 
    SELECT distinct * FROM ticker_pn
),
# fim: Criando uma tabela com os tickers e seus cnpjs

#-----------------------------------------------------------------------------------------------------

# início: Criando um dataset com os atributos das tabelas cad e  fundamentei
fundamentei_distinct AS (

SELECT 

	DISTINCT
	f.cnpj,
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

FROM fundamentei f 

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

LEFT JOIN fundamentei_distinct AS f 
ON cad.cnpj_cia = f.cnpj

WHERE cad.SIT = 'ATIVO' AND cad.TP_MERC = 'BOLSA'
    
),
# fim: Criando um dataset com os atributos das tabelas cad e fundamentei

#-----------------------------------------------------------------------------------------------------

# Início: incluindo cnpj na tabela fundamentus para fazer join com os atributos
fundamentus2 AS	(
SELECT 
	cnpj,
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
	f.liq_corr, 
	f.roic, 
	f.roe, 
	f.liq_2meses, 
	f.patrim_liq, 
	f.div_bruta_patrim, 
	f.cresc_rec_cinco_anos
FROM tickers
LEFT JOIN fundamentus f
ON ticker = papel 
	)
# Início: incluindo cnpj na tabela fundamentus para fazer join com os atributos
	
#-----------------------------------------------------------------------------------------------------
	
# Criando a tabela obt_b3 com todos os dados das tabelas fundamentus,
fundamentei, cad e dfp_cia_aberta_DRE_con_2023
	
SELECT
	-- ROW_NUMBER() over(ORDER BY f.cnpj) AS sk_obt,
	f.cnpj,
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
	f.liq_corr, 
	f.roic, 
	f.roe, 
	f.liq_2meses, 
	f.patrim_liq, 
	f.div_bruta_patrim, 
	f.cresc_rec_cinco_anos,
	a.*,
	dfp.DT_REFER, 
	dfp.VERSAO, 
	dfp.DENOM_CIA, 
	dfp.CD_CVM, 
	dfp.GRUPO_DFP, 
	dfp.MOEDA, 
	dfp.ESCALA_MOEDA, 
	dfp.ORDEM_EXERC, 
	dfp.DT_INI_EXERC, 
	dfp.DT_FIM_EXERC, 
	dfp.CD_CONTA, 
	dfp.DS_CONTA, 
	dfp.VL_CONTA, 
	dfp.ST_CONTA_FIXA
	
FROM fundamentus2 f
LEFT JOIN atributos a
ON f.cnpj = a.CNPJ_CIA
LEFT JOIN dfp_cia_aberta_DRE_con_2023 dfp
on f.CNPJ = dfp.CNPJ_CIA 
WHERE papel is not null;

	