from datetime import datetime
import pandas as pd, numpy as np, math
import streamlit as st
import google.cloud.bigquery.dbapi as bq
import json

f = open('settings.json')
dados = json.load(f)

cache_ttl = dados['ttl']

hoje = datetime.now().strftime("%Y-%m-%d")

#@st.cache(ttl=7200)
@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_resumo():
    resumo_posicao = pd.read_gbq(
        "select \
            posicao, count(distinct uc) as qtd_uc, sum(peso_de_carga) as peso_total \
        from \
            tryanon-362612.aquario.estoque_ewm_ot \
        where \
            planta = 'P703' \
        group by \
            posicao \
        order by \
            peso_total desc"
    )

    historico_estoque = pd.read_gbq(
        "select \
            data_arquivo, round((sum(peso_de_carga)/1000),2) as qtd_total \
        from \
            tryanon-362612.aquario.estoque_ewm \
        where \
            peso_de_carga > 0 and planta = 'P703' \
        group by \
            data_arquivo"
    )

    return resumo_posicao, historico_estoque

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_inv():
    inv = pd.read_gbq(
        "WITH lista_inv AS ( \
        SELECT \
            material, \
            uc, \
            lote, \
            peso_de_carga, \
            umb, \
            tipo_estoque, \
            FORMAT_DATE ('%d/%m/%Y',DATE(data_em)) as data_em, \
        FROM \
            tryanon-362612.aquario.estoque_ewm_ot ewm \
        WHERE \
            posicao = 'INV' and planta = 'P703') \
        SELECT \
            * \
            FROM \
                lista_inv \
        LEFT JOIN \
            tryanon-362612.aquario.status_inv status \
        ON \
            lista_inv.uc = status.uc")
    
    tipo_material = pd.read_gbq(
        "SELECT \
            distinct LTRIM(MaterialNumber_MATNR,'0') AS material, type_nm AS tipo, div_nm AS divisao \
        FROM \
            prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs \
        WHERE \
            Plant_WERKS = 'P703'"
            )

    inv = inv.drop(['uc_1','data_mov'],axis=1)
    
    ucs_inv = tuple(inv.uc.unique())

    mov_inv = pd.read_gbq(
        "select \
            tarefa, descr_processo, confirmador, data_confirmacao, pd_origem, pd_destino, peso_carga, material, lote, uc_destino \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            pd_destino = 'INV' and planta = 'P703' and descr_processo = 'Transferência interna de estoque' and uc_destino in "+str(ucs_inv))

    preco_item = pd.read_gbq("with a as (SELECT DISTINCT t1.Plant_WERKS, t1.MaterialNumber_MATNR, t1.Periodic_unit_price_VERPR, t1.Standard_price_STPRS, t1.Stk_Date\
        FROM prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs t1\
        JOIN (\
            SELECT Plant_WERKS, MaterialNumber_MATNR, MAX(Stk_Date) AS max_date\
            FROM prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs\
            WHERE Stk_Date < DATE_TRUNC(CURRENT_DATE, MONTH)\
            GROUP BY Plant_WERKS, MaterialNumber_MATNR\
        ) t2 ON t1.Plant_WERKS = t2.Plant_WERKS AND t1.MaterialNumber_MATNR = t2.MaterialNumber_MATNR AND t1.Stk_Date = t2.max_date\
        UNION ALL\
        SELECT DISTINCT t1.Plant_WERKS, t1.MaterialNumber_MATNR, t1.Periodic_unit_price_VERPR, t1.Standard_price_STPRS, t1.Stk_Date\
        FROM prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs t1\
        WHERE NOT EXISTS (\
            SELECT 1\
            FROM prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs t2\
            WHERE t1.Plant_WERKS = t2.Plant_WERKS AND t1.MaterialNumber_MATNR = t2.MaterialNumber_MATNR AND t2.Stk_Date < DATE_TRUNC(CURRENT_DATE, MONTH)\
        )\
        AND t1.Stk_Date = (\
            SELECT MAX(Stk_Date)\
            FROM prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs\
            WHERE Plant_WERKS = t1.Plant_WERKS AND MaterialNumber_MATNR = t1.MaterialNumber_MATNR AND Stk_Date < DATE_TRUNC(CURRENT_DATE, MONTH)\
        ))\
        select LTRIM(MaterialNumber_MATNR,'0') AS material, Standard_price_STPRS AS preco from a where Plant_WERKS = 'P703'")

    mov_inv.data_confirmacao = pd.to_datetime(mov_inv.data_confirmacao, format=('%Y/%m/%d'))
    mov_inv.sort_values('data_confirmacao', inplace=True)
    mov_inv['dias_posicao'] = (datetime.today() - mov_inv.data_confirmacao).dt.days
    mov_inv.drop_duplicates(subset=['uc_destino'], keep='last', inplace=True)
    mov_inv = mov_inv.merge(tipo_material, on='material', how='left')

    mov_inv = mov_inv.merge(preco_item, on='material', how='left')

    mov_inv['valor'] = mov_inv.peso_carga * mov_inv.preco

    return inv, mov_inv

def load_inv_atualizado():
    inv = pd.read_gbq(
        "WITH lista_inv AS ( \
        SELECT \
            material, \
            uc, \
            lote, \
            peso_de_carga, \
            umb, \
            tipo_estoque, \
            FORMAT_DATE ('%d/%m/%Y',DATE(data_em)) as data_em, \
        FROM \
            tryanon-362612.aquario.estoque_ewm_ot ewm \
        WHERE \
            posicao = 'INV' and planta = 'P703') \
        SELECT \
            * \
            FROM \
                lista_inv \
        LEFT JOIN \
            tryanon-362612.aquario.status_inv status \
        ON \
            lista_inv.uc = status.uc")

    inv = inv.drop(['uc_1','data_mov'],axis=1)


    return inv

def load_saida_inv(delta):

    data = str(delta)

    saida_inv = pd.read_gbq("SELECT material, uc_destino, lote, peso_carga, pd_origem, pd_destino, confirmador, data_confirmacao\
    FROM\
    tryanon-362612.aquario.tarefas\
    WHERE\
    planta = 'P703' and pd_origem = 'INV' AND pd_destino <> 'INV' AND descr_processo = 'Transferência interna de estoque' AND data_confirmacao LIKE '%"+ data +"%'")

    return saida_inv

def load_entrada_inv(delta):
    data = str(delta)

    entrada_inv = pd.read_gbq("SELECT material, uc_destino, lote, peso_carga, pd_origem, pd_destino, confirmador, data_confirmacao\
    FROM\
    tryanon-362612.aquario.tarefas\
    WHERE\
    planta = 'P703' and pd_origem <> 'INV' AND pd_destino = 'INV' AND descr_processo = 'Transferência interna de estoque' AND data_confirmacao LIKE '%"+ data +"%'")

    return entrada_inv

def ajusta_uc(row):
    uc_origem = row['uc_origem']
    uc_destino = row['uc_destino']

    if uc_origem != 'na' and uc_destino != 'na':
        return uc_destino
    elif uc_origem == uc_destino:
        return uc_destino
    elif uc_origem == 'na':
        return uc_destino
    elif uc_destino == 'na':
        return uc_origem

def load_paletes_transformados(delta):
    data = str(delta)

    dados_lotes = pd.read_gbq(
        "SELECT \
            lote, COUNT(DISTINCT material) AS unique_material \
        FROM \
            tryanon-362612.aquario.tarefas \
        where \
            (descr_processo IN ('Registro da saída de mercadoria','Registro da entrada de mercadorias')) \
            AND Date(data_criacao) = '" + data + "' \
        GROUP BY lote having unique_material = 2 \
    ")

    lotes_transf = dados_lotes.lote.unique()

    if len(lotes_transf) == 0:
            lotes_transf = tuple(['0','0'])
    elif len(lotes_transf) == 1:
        lotes_transf = np.append(lotes_transf, 'a')
        lotes_transf = tuple(lotes_transf)
    else:
        lotes_transf = tuple(lotes_transf)

    #lotes_transf = tuple(dados_lotes.lote.unique())

    paletes_transformados = pd.read_gbq(
        "SELECT \
            tarefa, descr_processo, Date(data_criacao) AS data_criacao, hora_criacao, material, peso_carga, lote, pd_origem, pd_destino, uc_origem, uc_destino \
        FROM \
            tryanon-362612.aquario.tarefas \
        where \
            (descr_processo IN ('Registro da saída de mercadoria','Registro da entrada de mercadorias')) \
            AND lote IN " + str(lotes_transf) + " \
            AND Date(data_criacao) = '" + data + "' \
        ")

    if paletes_transformados.tarefa.nunique() > 0:
        paletes_transformados['uc'] = paletes_transformados.apply(ajusta_uc, axis=1)
        paletes_transformados = paletes_transformados.drop(columns=['uc_origem','uc_destino'])

    result = dados_lotes.merge(paletes_transformados, on='lote', how='right')
    result = result.loc[result.lote != 'na']

    return result.sort_values('lote')

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_resumo_mov_inv():
    resumo_mov_inv = pd.read_gbq("WITH saida as (SELECT tarefa, descr_processo, confirmador, data_confirmacao, pd_origem, pd_destino, peso_carga, material, lote, uc_destino,\
        'saida' AS direcao, DATE_DIFF(CURRENT_DATE(), DATE(data_criacao), DAY) diff_days\
        FROM\
        tryanon-362612.aquario.tarefas\
        WHERE\
        planta = 'P703' and pd_origem = 'INV' AND pd_destino <> 'INV' AND descr_processo = 'Transferência interna de estoque'),\
        entrada AS (SELECT tarefa, descr_processo, confirmador, data_confirmacao, pd_origem, pd_destino, peso_carga, material, lote, uc_destino,\
        'entrada' as direcao, DATE_DIFF(CURRENT_DATE(), DATE(data_criacao), DAY) diff_days\
        FROM\
        tryanon-362612.aquario.tarefas\
        WHERE\
        planta = 'P703' AND pd_origem <> 'INV' AND pd_destino = 'INV' AND descr_processo = 'Transferência interna de estoque')\
        SELECT * FROM saida\
        UNION ALL\
        SELECT * FROM entrada\
        "
    )

    resumo_mov_inv = resumo_mov_inv.loc[resumo_mov_inv.diff_days <= 7].groupby(['data_confirmacao','direcao','material'])['peso_carga'].sum().to_frame().reset_index()

    tipo_material = pd.read_gbq(
        "SELECT \
            distinct LTRIM(MaterialNumber_MATNR,'0') AS material, type_nm AS tipo, div_nm AS divisao \
        FROM \
            prd-valgroup-dna.corp_trs_stock.stk_vw_balances_hist_trs \
        WHERE \
            Plant_WERKS = 'P703'"
            )

    resumo_mov_inv = resumo_mov_inv.merge(tipo_material, on='material', how='left')

    return resumo_mov_inv

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)

def load_SPP(inv):
    lotes_inv = inv.lote.unique()
    lotes_inv = np.delete(lotes_inv, np.where(lotes_inv == 'na'))
    lotes_inv = tuple(lotes_inv)

    lote_hist = pd.read_gbq(
        "select \
            material, lote, pd_origem, pd_destino, uc_origem \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            descr_processo = 'Transferência interna de estoque' and \
            pd_origem not in ('GR-PROD','GR-YDI1','INV') and \
            pd_destino not in ('GR-PROD','GR-YDI1','INV') and \
            lote in " + str(lotes_inv) \
            + "order by \
            lote, pd_origem, pd_destino"
    )

    lote_hist.rename(columns={'uc_origem':'uc'}, inplace=True)

    hist_origem = lote_hist[['pd_origem','uc','material']]
    hist_origem.rename(columns={'pd_origem':'posicao'}, inplace=True)

    hist_destino = lote_hist[['pd_destino','uc','material']]
    hist_destino.rename(columns={'pd_destino':'posicao'}, inplace=True)

    hist_geral = pd.concat([hist_origem,hist_destino])
    hist_geral = hist_geral.drop_duplicates(subset=(['posicao','uc'])).sort_values(['posicao'])

    return hist_geral

def load_SPP_byFile(file):

    dados = pd.read_excel(file, dtype={'Lote':'string'})

    dados = dados.fillna('na')

    lotes_inv = dados.Lote.unique()
    lotes_inv = tuple(lotes_inv)
        
    lote_hist = pd.read_gbq(
        "select \
            material, lote, pd_origem, pd_destino, uc_origem, Date(data_criacao) AS data_criacao \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            descr_processo = 'Transferência interna de estoque' and \
            pd_origem not in ('GR-PROD','GR-YDI1','INV') and \
            pd_destino not in ('GR-PROD','GR-YDI1','INV') and \
            lote in " + str(lotes_inv) \
            + "order by \
            lote, pd_origem, pd_destino, Date(data_criacao)"
    )

    lote_hist.rename(columns={'uc_origem':'uc'}, inplace=True)

    hist_origem = lote_hist[['pd_origem','uc','material','data_criacao']]
    hist_origem.rename(columns={'pd_origem':'posicao'}, inplace=True)

    hist_destino = lote_hist[['pd_destino','uc','material','data_criacao']]
    hist_destino.rename(columns={'pd_destino':'posicao'}, inplace=True)

    hist_geral = pd.concat([hist_origem,hist_destino])
    hist_geral = hist_geral.drop_duplicates(subset=(['posicao','uc'])).sort_values(['posicao'])

    return hist_geral

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_str_mov_inv():
    movs_inv = pd.read_gbq(
    "SELECT \
        tarefa, pd_origem, pd_destino, material, data_criacao, hora_criacao,\
        CONCAT(pd_origem, ' > ', pd_destino) as str_mov,\
        CASE pd_origem\
            WHEN 'INV' THEN 'saida'\
            ELSE 'entrada'\
        END as direcao\
    FROM\
        tryanon-362612.aquario.tarefas\
    WHERE\
        planta = 'P703' AND\
        pd_origem <> pd_destino AND\
        pd_origem <> 'na' AND pd_destino <> 'na' AND\
        (pd_origem = 'INV' OR pd_destino = 'INV') AND\
        descr_processo = 'Transferência interna de estoque'"
    )
    
    #Date(data_criacao) > '2022-06-01' AND\

    resumo_str_mov = movs_inv.groupby([movs_inv.pd_origem, movs_inv.pd_destino, movs_inv.str_mov, movs_inv.direcao, movs_inv.data_criacao]).count()
    resumo_str_mov = resumo_str_mov[['tarefa']].sort_values('tarefa', ascending=False).reset_index()

    return resumo_str_mov

def load_racks_analyser(df):
    dados = pd.read_excel(df, usecols=(['Posição no depósito','Produto','Unidade comercial','Peso de carga']), dtype={'Unidade comercial':'string'})

    dados = dados.rename(columns={
        'Posição no depósito':'posicao',
        'Produto':'produto',
        'Unidade comercial':'uc',
        'Peso de carga':'peso_carga'
    })
    dados = dados.drop_duplicates(subset=('uc'))
    
    pesos_uc = dados.groupby('uc')['peso_carga'].sum().to_frame().reset_index()

    dados['get_detail'] = dados.posicao.apply(lambda x: 'rack' if len(x) == 6 and x.count('-') == 2 else 'na')
    dados['altura'] = dados.posicao.str[5]
    dados = dados.loc[(dados.get_detail == 'rack') & (dados.altura != '1')]
    dados['rua'] = dados.posicao.str[:1]
    dados['rack'] = dados.posicao.str[2:4].astype('int')
    dados['rack_ordem'] = dados.rack.apply(lambda x: x-31 if x > 31 else x)
    dados = dados.sort_values(['rua','rack_ordem','rack','altura'])

    cont_pos = dados.posicao.to_frame()
    cont_pos = cont_pos.groupby('posicao')['posicao'].count().to_frame().rename(columns={'posicao':'qtd_pos'}).reset_index()
    cont_pos = cont_pos.loc[cont_pos.qtd_pos > 2].reset_index()
    cont_pos = cont_pos[['posicao','qtd_pos']]

    check = dados.merge(cont_pos, on='posicao', how='left')
    check = check.loc[check.qtd_pos >= 3]
    check = check[['posicao','produto','uc']]

    check = check.merge(pesos_uc, on='uc', how='left')

    return check

def load_racks_analyser_rua(df, inicio='fundo'):
    dados = pd.read_excel(df, usecols=(['Posição no depósito','Produto','Unidade comercial','Peso de carga']), dtype={'Unidade comercial':'string'})

    dados = dados.rename(columns={
        'Posição no depósito':'posicao',
        'Produto':'produto',
        'Unidade comercial':'uc',
        'Peso de carga':'peso_carga'
    })

    pesos_uc = dados.groupby('uc')['peso_carga'].sum().to_frame().reset_index()

    dados = dados.drop_duplicates(subset=('uc'))

    dados['get_detail'] = dados.posicao.apply(lambda x: 'rack' if len(x) == 6 and x.count('-') == 2 else 'na')
    dados['altura'] = dados.posicao.str[5]
    dados = dados.loc[(dados.get_detail == 'rack')]
    dados['rua'] = dados.posicao.str[:1]
    dados['rack'] = dados.posicao.str[2:4].astype('int')
    dados['rack_ordem'] = dados.rack.apply(lambda x: x-31 if x > 31 else x)

    if inicio == 'frente':
        dados = dados.sort_values(['rua','rack_ordem','rack','altura'])
    else:
        dados = dados.sort_values(['rua','rack_ordem','rack','altura'], ascending=[True,False,False,True])
    
    dados = dados[['posicao','produto','uc']]

    dados = dados.merge(pesos_uc, on='uc', how='left')

    return dados

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_positions():
    posicoes = pd.read_excel('dados_st/posicoes_p3.xlsx')

    return posicoes

def load_uc_path(batch):

    posicoes = load_positions()

    rastreio = pd.read_gbq(
    "SELECT \
        distinct tarefa, descr_processo, autor, data_criacao, hora_criacao, pd_origem, pd_destino, lote, uc_destino\
    FROM\
        tryanon-362612.aquario.tarefas\
    WHERE\
        (descr_processo = 'Transferência interna de estoque'\
        OR\
        descr_processo = 'Registro da entrada de mercadorias'\
        OR\
        descr_processo = 'Registro da saída de mercadoria'\
        OR\
        descr_processo = 'Saída de depósito'\
        )\
    AND\
        pd_origem <> pd_destino\
    AND\
        lote = '" + str(batch) + "'"
    )

    rastreio = rastreio.sort_values(['data_criacao','hora_criacao'])

    rastreio_2 = rastreio.merge(
        posicoes, left_on='pd_origem', right_on='posicao_deposito', how='left').merge(
            posicoes, left_on='pd_destino', right_on='posicao_deposito', how='left')

    rastreio_2 = rastreio_2.fillna('na')

    rastreio_2['caminho'] = rastreio_2.uso_posicao_x + '>' + rastreio_2.uso_posicao_y
    rastreio_2 = rastreio_2.drop(columns=(['posicao_deposito_x','uso_posicao_x','posicao_deposito_y']))
    rastreio_2 = rastreio_2.rename(columns={
        'tipo_posicao_x':'tipo_pd_origem',
        'tipo_posicao_y':'tipo_pd_destino'
        })

    return rastreio_2

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_rec():
    rec = pd.read_gbq(
        "select \
        posicao, material, uc, lote, quantidade, peso_de_carga, umb, nome_tipo_estoque, data_em, \
            from \
        tryanon-362612.aquario.estoque_ewm_ot \
            where \
        posicao like 'REC%' and data_arquivo like '%"+ hoje +"%'"
        )
    
    ucs_rec = rec.uc.unique()

    if len(ucs_rec) == 0:
            ucs_rec = tuple(['0','0'])
    elif len(ucs_rec) == 1:
        ucs_rec = np.append(ucs_rec, 'a')
        ucs_rec = tuple(ucs_rec)
    else:
        ucs_rec = tuple(rec.uc.unique())

    mov_rec = pd.read_gbq(
        "select \
            tarefa, descr_processo, confirmador, data_confirmacao, pd_origem, pd_destino, material, lote, uc_destino \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            pd_destino like 'REC%' and pd_origem not like 'REC%' and descr_processo = 'Transferência interna de estoque' and uc_destino in " + str(ucs_rec))
    
    mov_rec.data_confirmacao = pd.to_datetime(mov_rec.data_confirmacao, format=('%Y/%m/%d'))
    mov_rec.sort_values('data_confirmacao', inplace=True)
    mov_rec['dias_posicao'] = (datetime.today() - mov_rec.data_confirmacao).dt.days

    return rec, mov_rec

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_ydi():
    ydi = pd.read_gbq(
        "select \
            posicao, material, uc, lote, quantidade, peso_de_carga, umb, nome_tipo_estoque, data_em \
        from \
            tryanon-362612.aquario.estoque_ewm_ot \
        where posicao like 'GR-YDI1' and data_arquivo like '%" + hoje + "%'"
    )

    return ydi

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=cache_ttl)
def load_rep():
    rep = pd.read_gbq(
        "select \
            material, uc, lote, quantidade, peso_de_carga, umb, tipo_estoque, data_em, \
        from \
            tryanon-362612.aquario.estoque_ewm_ot \
        where \
            posicao like 'REP%' and data_arquivo like '%"+ hoje +"%'"
        )
    rep.data_em = pd.to_datetime(rep.data_em, format=('%Y/%m/%d'))

    #corrigir a fórmula para buscar o momento que a UC foi enviada para o REP
    rep['dias_posicao'] = (datetime.today() - rep.data_em).dt.days
    
    rep.sort_values('dias_posicao', inplace=True, ascending=False)
    
    ucs_rep = tuple(rep.uc.unique())

    if len(ucs_rep) == 0:
        ucs_rep = tuple(['0','0'])
    elif len(ucs_rep) == 1:
        ucs_rep = np.append(ucs_rep, 'a')
        ucs_rep = tuple(ucs_rep)
    else:
        ucs_rep = tuple(rep.uc.unique())

    mov_rep = pd.read_gbq(
        "select \
            tarefa, descr_processo, confirmador, data_confirmacao, pd_origem, pd_destino, material, lote, uc_destino \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            pd_destino like 'REP%' and pd_origem not like 'REP%' and descr_processo = 'Transferência interna de estoque' and uc_destino in " + str(ucs_rep))

    return rep, mov_rep

@st.cache(allow_output_mutation=True, ttl=cache_ttl)
#@st.cache_data(ttl=3600)
def load_gr_prod():
    gr_prod = pd.read_gbq(
    "select \
        material, uc, lote, quantidade, peso_de_carga, umb, tipo_estoque, data_em, \
    from \
        tryanon-362612.aquario.estoque_ewm_ot \
    where \
        posicao like 'GR-PROD'"
    )

    gr_prod.data_em = pd.to_datetime(gr_prod.data_em, format=('%Y/%m/%d'))
    gr_prod['dias_posicao'] = (datetime.today() - gr_prod.data_em).dt.days

    aap_lop3 = pd.read_gbq(
        "with a as (SELECT\
        *\
        FROM\
        tryanon-362612.aquario.tarefas\
        WHERE\
        uc_destino IN (\
        SELECT\
            DISTINCT uc\
        FROM\
            tryanon-362612.aquario.estoque_ewm_ot\
        WHERE\
            posicao = 'AAP-LOP3-SA1')\
        AND\
        pd_destino = 'AAP-LOP3-SA1'\
        AND\
        pd_origem NOT IN ('AAP-LOP3-SA1', 'na'))\
        select material, uc_destino, lote, peso_carga, umb, tipo_estoque, data_criacao, ROW_NUMBER() OVER(PARTITION BY material ORDER BY DATE(data_criacao) DESC) as item FROM a"
    )

    #aap_lop3 = pd.read_gbq(
    #"select \
    #    material, uc, lote, quantidade, peso_de_carga, umb, tipo_estoque, data_em, \
    #from \
    #    tryanon-362612.aquario.estoque_ewm_ot \
    #where \
    #    posicao like 'AAP-LOP3-SA1'"
    #)

    aap_lop3.data_criacao = pd.to_datetime(aap_lop3.data_criacao, format=('%Y/%m/%d'))
    aap_lop3['dias_posicao'] = (datetime.today() - aap_lop3.data_criacao).dt.days
    aap_lop3['data_criacao'] = aap_lop3['data_criacao'].replace('T00:00:00','')

    return gr_prod, aap_lop3

def busca_tarefas(uc):
    dados_tarefas = pd.read_gbq(
        "select \
            tarefa, descr_processo, autor, data_criacao, hora_criacao, pd_origem, pd_destino, material, lote, peso_carga, uc_destino, uc_origem, tipo_estoque, documento \
        from \
            tryanon-362612.aquario.tarefas \
        where \
            uc_destino = '" + str(uc) + "' or uc_origem = '" + str(uc) + "' order by data_criacao desc, hora_criacao desc")

    return dados_tarefas

def downloadToExcel(df):
    from io import BytesIO
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    writer.save()
    processed_data = output.getvalue()
    return processed_data

def executeQueryBq(query):
    try:
        con = bq.connect()
        cursor = con.cursor()
        #query = "DELETE  FROM `tryanon-362612.aquario.activities` WHERE arquivo = 'mb52 07-09.XLSX'"
        cursor.execute(query)
        con.commit()
        con.close()
    except Exception as e:
        str(e)

def load_remessas(data, tipo='remessa'):

    if len(data) == 0:
        data = tuple(['0','0'])
    elif len(data) == 1:
        data = np.append(data, 'a')
        data = tuple(data)
    else:
        data = tuple(data)

    if tipo != 'remessa':
        data = pd.read_gbq(
            "SELECT \
                distinct LTRIM(delv_doc_cd,'0') As remessa \
            FROM \
                prd-valgroup-dna.corp_trs_sales.bill_vw_trs \
            WHERE \
                LTRIM(nf_cd,'0') IN " + str(data)
        )
        
        data = data.remessa.unique()

        if len(data) == 0:
            data = tuple(['0','0'])
        elif len(data) == 1:
            data = np.append(data, 'a')
            data = tuple(data)
        else:
            data = tuple(data)
    else:
        pass


    dados = pd.read_gbq(
        "SELECT \
            LTRIM(MaterialNumber_MATNR,'0') As material, LTRIM(Delivery_VBELN,'0') As inbound, LTRIM(ExternalIdentificationOfDeliveryNote_LIFEX,'0') As remessa, BatchNumber_CHARG As lote, ActualQuantityDelivered_InSalesUnits_LFIMG As quantidade \
        FROM \
            prd-valgroup-dna.corp_trs_sales.deliveries_inbound_vw_trs \
        WHERE \
            ActualQuantityDelivered_InSalesUnits_LFIMG > 0 AND LTRIM(Delivery_VBELN, '0') IN " + str(data) + " OR LTRIM(ExternalIdentificationOfDeliveryNote_LIFEX, '0') IN " + str(data)
        )

    lotes = dados.lote.unique()

    if len(lotes) == 0:
        lotes = tuple(['0','0'])
    elif len(lotes) == 1:
        lotes = np.append(lotes, 'a')
        lotes = tuple(lotes)
    else:
        lotes = tuple(lotes)

    dados_tarefas = pd.read_gbq(
        "SELECT \
            lote, uc, tara, peso_bruto, volume \
        FROM \
            tryanon-362612.aquario.zmon \
        WHERE \
            lote IN " + str(lotes) \
    )

    dados = dados.merge(dados_tarefas, on='lote', how='left')

    dados = dados.sort_values(['inbound','remessa'])

    dados = dados.loc[dados.quantidade > 0].drop_duplicates()

    return dados

def load_de_para(material):
    de_para = pd.read_gbq(
                    'WITH \
            a AS( \
            SELECT \
                DISTINCT material AS semi_acabado, \
                ordem \
            FROM \
                tryanon-362612.aquario.mb51 \
            WHERE \
                tipo_movimento = "261" \
                AND ordem LIKE "33%" \
                AND quantidade < 0 \
                AND (material LIKE "SZ%" \
                OR material LIKE "HZ%")), \
            b AS( \
            SELECT \
                DISTINCT material AS acabado, \
                ordem \
            FROM \
                tryanon-362612.aquario.mb51 \
            WHERE \
                tipo_movimento = "101" \
                AND ordem LIKE "33%" \
                AND quantidade > 0 \
                AND (material LIKE "HA%" \
                OR material LIKE "SA%")), \
            c AS ( \
            SELECT \
                * EXCEPT (ordem) \
            FROM \
                a \
            LEFT JOIN \
                b \
            ON \
                a.ordem = b.ordem) \
            SELECT \
            DISTINCT * \
            FROM \
            c \
            where acabado is not null \
            and (acabado = "' + str(material) + '" or semi_acabado = "' + str(material) +'")'
        )
    
    return de_para

class Cativa():
    '''
    O dataframe precisa conter as colunas originais do arquivo baixado do SAP...
    '''
    def __init__(self, df):
        self.df = df

        dados = pd.read_excel(df, usecols=(['Posição no depósito','Produto','Unidade comercial']), dtype={'Unidade comercial':'string'})

        dados_df = dados.rename(columns={
            'Posição no depósito':'posicao',
            'Produto':'produto',
            'Unidade comercial':'uc'
        })

        dados_df['tipo_posicao'] = dados_df.posicao.apply(lambda x: 'rack' if len(x) == 6 and x.count('-') == 2 else 'na')

        dados_df = dados_df.loc[dados_df.tipo_posicao == 'rack']

        map_rua = {'A':10,'B':9,'C':8,'D':7,'E':6,'F':5,'G':4,'H':3,'I':2,'J':1,'K':0}
        map_blocado = 0

        dados_df['rua'] = dados_df.posicao.str[:1]
        dados_df['rack'] = pd.to_numeric(dados_df.posicao.str[2:4])
        dados_df['x_rua'] = dados_df.rua.map(map_rua)
        dados_df['y_rua'] = dados_df.rack.apply(lambda x: x if x <= 31 else x - 31)
        dados_df['altura'] = dados_df.posicao.str[-1]
        dados_df['y'] = abs(dados_df.y_rua - 31)
        dados_df = dados_df[['posicao','produto','uc','x_rua','y']]
        dados_df['coord'] = dados_df.x_rua.astype(str) + ',' + dados_df.y.astype(str)

        self.df = dados_df

    def ic_ideal(self, item: str):
        '''ix_ideal = n_ucs / capacidade_rack'''
        df_item = self.df.loc[self.df.produto == item]

        n_ucs = len(df_item.uc.unique())
        ix_ideal = n_ucs / 14

        return ix_ideal

    def ic_real(self, item: str, origin='dinamic_relative', x1=0, y1=0):
        '''
        Tipos de origens:
        - dinamic_relative: Mede a distância do próximo palete referente ao último medido.
        - fixed_relative: Mede a distância do próximo palete em relação ao primeiro da lista.
        - fixed: Mede a distância de cada palete em relação à um ponto fixo definido na chamada da função.
        '''
        ix_real = 0

        df_item = self.df.loc[self.df.produto == item].sort_values(['x_rua','y'], ascending = [True, True])
        lista_coord = df_item.coord.unique()
        
        if origin == 'fixed_relative':
            x1, y1 = int(coord.split(',')[0]), int(coord.split(',')[1])
            
        for coord in lista_coord:
            x2 = int(coord.split(',')[0])
            y2 = int(coord.split(',')[1])

            ix_real += math.sqrt((x2-x1)**2 + (y2-y1)**2)

            if origin == 'dinamic_relative':
                x1, y1 = x2, y2

        return ix_real