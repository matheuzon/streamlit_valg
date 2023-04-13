from datetime import datetime
import numpy as np
import plotly.express as px
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from func_estoque import *

# Ambiente Streamlit
st.set_page_config(page_title='Análise estoque', page_icon=':bar_chart:', layout='wide')

# Estilos
st.markdown("""
        <style>
               .css-18e3th9 {
                    padding-top: 2rem;
                    padding-bottom: 10rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                }
               .css-1d391kg {
                    padding-top: 3.5rem;
                    padding-right: 1rem;
                    padding-bottom: 3.5rem;
                    padding-left: 1rem;
                }
        </style>
        """, unsafe_allow_html=True)

hoje = datetime.now().strftime("%Y-%m-%d")

tab_home, tab_inv, tab_spp, tab_rec, tab_ydi, tab_gr_prod, tab_rep, tab_stock, tab_bot = st.tabs([
    'HOME', 'INV', 'SPP', 'REC', 'GR-YDI1', 'PRODUÇÃO', 'REP', 'STOCK', 'BOT'
])

##inv, mov_inv = load_inv()

with tab_home:
    resumo_posicao, historico_estoque = load_resumo()
    st.dataframe(resumo_posicao)

    ax_historico = px.area(historico_estoque, x='data_arquivo', y='qtd_total')
    #ax_historico.update_xaxes(showticklabels=False)
    #ax_historico.update_yaxes(showticklabels=False)
    st.plotly_chart(ax_historico)

with tab_inv:##################################################################################################################
    inv, mov_inv = load_inv()

    exp_pos_inv = st.expander('Posição INV 👉', expanded=False)
    exp_mov_inv = st.expander('Movimentação INV 👉', expanded=False)
    exp_saida_inv = st.expander('Entrada/Saída INV 👉', expanded=False)

    with exp_pos_inv:
        col1, col2, col3 = st.columns(3)

        with col1:
            total_ucs = str(len(inv.uc.unique()))
            st.text('Total UCs: '+total_ucs)
        with col2:
            total_peso = str(round(inv.peso_de_carga.sum(),2))
            st.text('Total peso: '+total_peso)
        with col3:
            download_xlsx_pos_inv = downloadToExcel(inv)
            st.download_button(label = 'Baixar tabela', data = download_xlsx_pos_inv, file_name='dados_posicao_inv.xlsx')
            
        
        opt_atualizar_status_inv = st.checkbox('Atualizar status')
        if opt_atualizar_status_inv == True:
            col1_status, col2_status, col3_status = st.columns(3)
            with col1_status:
                atualizar_uc = st.text_input('Inserir UC')
                atualizar_uc = str(atualizar_uc)
            with col2_status:
                atualizar_motivo = st.text_input('Motivo')
                atualizar_motivo = str(atualizar_motivo)
            with col3_status:
                atualizar_responsavel = st.text_input('Responsável')
                atualizar_responsavel = str(atualizar_responsavel)

            if st.button('Atualizar base'):
                if atualizar_uc != '':
                    if atualizar_motivo != '':
                        executeQueryBq("UPDATE `tryanon-362612.aquario.status_inv` SET obs ='"+atualizar_motivo+"', responsavel = '"+atualizar_responsavel+"' WHERE uc ='"+atualizar_uc+"'")
                        inv = load_inv_atualizado()
                        st.warning('base atualizada')
        #st.dataframe(inv)
        AgGrid(inv, fit_columns_on_grid_load=True, height=400)

    with exp_mov_inv:
        inv, mov_inv = load_inv()
        col1_mov_inv, col2_mov_inv = st.columns(2)

        with col1_mov_inv:
            download_xlsx_inv = downloadToExcel(mov_inv)
            busca_uc = st.text_input('UC:')

            if busca_uc != '':
                mov_inv = mov_inv.loc[mov_inv.uc_destino == busca_uc]
        
        with col2_mov_inv:
            st.download_button(label = 'Baixar tabela', data = download_xlsx_inv, file_name='dados_posicao_inv.xlsx')
        
        st.write(str(round(mov_inv.valor.sum(),2)))
        st.dataframe(mov_inv.drop(columns=(['tipo','divisao','preco'])))
        st.dataframe(mov_inv.groupby('material')['valor'].sum().to_frame().reset_index().sort_values('valor', ascending=False))

        axx = px.bar(
            mov_inv.groupby(['tipo','divisao'])['peso_carga'].sum().to_frame().reset_index(),
            x='divisao',
            y='peso_carga',
            color='tipo',
            barmode='group',
            text_auto='.2s',
            title='INV por item e divisão',
            width=900
        )

        axx.update_yaxes(showgrid=False, showticklabels=False)
        axx.update_layout(xaxis_title='Tipo do material', yaxis_title='Quantidade')
        
        st.plotly_chart(axx)

        check_tarefas = st.checkbox('Mostrar tarefas')

        if check_tarefas == True:
            tarefas_uc = busca_tarefas(busca_uc)
            st.dataframe(tarefas_uc)

    with exp_saida_inv:

        data = st.date_input('Data')

        saida_inv = load_saida_inv(data)
        entrada_inv = load_entrada_inv(data)
        
        st.write('Saída inv: ' + str(len(saida_inv.uc_destino.unique())) + ' paletes (' + str(saida_inv.peso_carga.sum()) + ' kg)')
        st.write('Entrada inv: ' + str(len(entrada_inv.uc_destino.unique())) + ' paletes (' + str(entrada_inv.peso_carga.sum()) + ' kg)')

        download_xlsx_inv_saida = downloadToExcel(saida_inv)
        st.download_button(label = 'Baixar tabela Saída', data = download_xlsx_inv_saida, file_name='dados_saida_inv.xlsx')

        st.dataframe(saida_inv)

        download_xlsx_inv_entrada = downloadToExcel(entrada_inv)
        st.download_button(label = 'Baixar tabela Entrada', data = download_xlsx_inv_entrada, file_name='dados_entrada_inv.xlsx')

        st.dataframe(entrada_inv)

        resumo_mov_inv = load_resumo_mov_inv()

        #inv_grafico = resumo_mov_inv.loc[resumo_mov_inv.diff_days <= 7].groupby(['data_confirmacao','direcao'])['peso_carga'].sum().to_frame().reset_index()

        filtro_inv = st.checkbox('Exibir por tipo/divisao')

        if filtro_inv == True:
            ax_inv = px.bar(
                resumo_mov_inv.groupby(['data_confirmacao','divisao','direcao'])['peso_carga'].sum().to_frame().reset_index(),
                x='data_confirmacao',
                y='peso_carga',
                color='direcao',
                barmode='group',
                text_auto='.2s',
                hover_data=['divisao'],
                title='Quantidade entrada/saída do INV',
                width=900,
                color_discrete_map={
                    'entrada': 'coral',
                    'saida': 'chartreuse',
                }

            )
            ax_inv.update_yaxes(showgrid=False, showticklabels=False)
            ax_inv.update_layout(xaxis_title='Data', yaxis_title='Quantidade')

        else:
            ax_inv = px.bar(
                resumo_mov_inv.groupby(['data_confirmacao','direcao'])['peso_carga'].sum().to_frame().reset_index(),
                x='data_confirmacao',
                y='peso_carga',
                color='direcao',
                barmode='group',
                text_auto='.2s',
                title='Quantidade entrada/saída do INV',
                width=900,
                color_discrete_map={
                    'entrada': 'coral',
                    'saida': 'chartreuse'
                }

            )
            ax_inv.update_yaxes(showgrid=False, showticklabels=False)
            ax_inv.update_layout(xaxis_title='Data', yaxis_title='Quantidade')
            #ax_inv.update_xaxes(showticklabels=False)
            #ax_inv.update_yaxes(showticklabels=False)

        st.plotly_chart(ax_inv)

with tab_spp:
    inv, mov_inv = load_inv()
    hist_geral = load_SPP(inv)
    resumo_str_mov = load_str_mov_inv()

    exp_spp = st.expander('Análise SPP 👉', expanded=False)
    exp_most_pos = st.expander('Maiores posições X-INV 👉', expanded=False)
    exp_analisa_altura = st.expander('Gerador de arquivos para conferência 👉', expanded=False)
    exp_rastreio = st.expander('Caminho da UC 👉', expanded=False)
    exp_transformacao = st.expander('Transformação de UCs 👉', expanded=False)

    with exp_spp:
        col_spp_1, col_spp_2 = st.columns(2)

        with col_spp_1:
            download_xlsx_hist_geral = downloadToExcel(hist_geral)
            st.download_button(label = 'Baixar tabela', data = download_xlsx_hist_geral, file_name='dados_hist_geral.xlsx')
            st.dataframe(hist_geral)


        with col_spp_2:
            st.write('Irá exibir uma análise básica inicial dos itens no INV.')
            st.write('- O tempo na posição.')
            st.write('- As posições em que estiveram no passado.')
            st.write('- Serão consideradas apenas posições que existem fisicamente.')
            
            st.subheader('Gera análise com o histórico dos lotes contidos no arquivo')
            st.write('O arquivo deve conter as colunas: [Posição no depósito,Produto,Unidade comercial,Peso de carga]')
            excel_ef = st.file_uploader('Arquivo com ucs', type=['xlsx'])

            if excel_ef:
                df_ef = load_SPP_byFile(excel_ef)
                download_xlsx_analise_racks_file = downloadToExcel(df_ef)
                st.download_button(label = 'Baixar tabela', data = download_xlsx_analise_racks_file, file_name='dados_analise_SPP_File.xlsx')
                st.dataframe(df_ef)

    with exp_most_pos:
        search_pos = st.text_input('Posicao')
        search_pos = search_pos.upper()

        if search_pos != "":
            search_pos_split = search_pos.split(':')
            st.write(search_pos_split)
            if search_pos.split(':')[0] == 'CONTAIN':
                resumo_str_mov = resumo_str_mov.loc[
                (resumo_str_mov.pd_origem.str.contains(search_pos_split[1]))
                |
                (resumo_str_mov.pd_destino.str.contains(search_pos_split[1]))
                ]
            else:
                #resumo_str_mov = resumo_str_mov.loc[resumo_str_mov.str_mov.str.contains(search_pos.upper())]
                resumo_str_mov = resumo_str_mov.loc[
                    (resumo_str_mov.pd_origem == search_pos)
                    |
                    (resumo_str_mov.pd_destino == search_pos)
                ]

        col1_most_pos, col2_most_pos = st.columns(2)

        resumo_str_mov_filtrado = resumo_str_mov[['str_mov','direcao','tarefa']]
        resumo_str_mov_filtrado = resumo_str_mov_filtrado.groupby(
            [resumo_str_mov_filtrado.str_mov,resumo_str_mov_filtrado.direcao])['tarefa'].sum().reset_index().sort_values(
                'tarefa', ascending=False)

        with col1_most_pos:
            st.write('Entradas no INV')
            download_xlsx_entradas_inv = downloadToExcel(resumo_str_mov_filtrado.loc[resumo_str_mov_filtrado.direcao == 'entrada'])
            st.download_button(label = 'Baixar tabela', data = download_xlsx_entradas_inv, file_name='dados_analise_entradas_inv.xlsx')
            st.dataframe(resumo_str_mov_filtrado.loc[resumo_str_mov_filtrado.direcao == 'entrada'])
        
        with col2_most_pos:
            st.write('Saídas no INV')
            download_xlsx_saidas_inv = downloadToExcel(resumo_str_mov_filtrado.loc[resumo_str_mov_filtrado.direcao == 'saida'])
            st.download_button(label = 'Baixar tabela', data = download_xlsx_saidas_inv, file_name='dados_analise_saidas_inv.xlsx')
            st.dataframe(resumo_str_mov_filtrado.loc[resumo_str_mov_filtrado.direcao == 'saida'])
    
    #Mostrar gráfico por posição
        st.dataframe(resumo_str_mov)
        ax_resumo = px.bar(resumo_str_mov, x='data_criacao', y='tarefa')
        #ax_historico.update_xaxes(showticklabels=False)
        #ax_historico.update_yaxes(showticklabels=False)
        st.plotly_chart(ax_resumo)

    with exp_analisa_altura:

        col1_analisa_arquivo, col2_analisa_arquivo = st.columns(2)

        with col1_analisa_arquivo:
            st.subheader('Gera arquivo com as posições do rack com mais de 3 paletes')
            excel_ef = st.file_uploader('Arquivo Estoque Físico', type=['xlsx'])

            if excel_ef:
                df_ef = load_racks_analyser(excel_ef)
                download_xlsx_analise_racks = downloadToExcel(df_ef)
                st.download_button(label = 'Baixar tabela', data = download_xlsx_analise_racks, file_name='dados_analise_racks_3_paletes.xlsx')
                st.dataframe(df_ef)

        with col2_analisa_arquivo:
            st.subheader('Gera arquivo com as posições em ordem para conferência')
            excel_ef_2 = st.file_uploader('Arquivo Estoque Físico por rua', type=['xlsx'])

            if excel_ef_2:
                df_ef = load_racks_analyser_rua(excel_ef_2)
                download_xlsx_analise_racks = downloadToExcel(df_ef)
                #st.radio('Escolha a ordem das posições', ['Frente','Fundo'])
                st.download_button(label = 'Baixar tabela', data = download_xlsx_analise_racks, file_name='dados_conferencia_racks_ruas.xlsx')
                
                st.dataframe(df_ef)

    with exp_rastreio:
        st.write('Rastreio de lotes e UCs')
        st.write('1 - recebimento > transicao')
        st.write('2 - transicao > armazenagem')
        st.write('3 - armazenagem > expedicao')
        lote_rastreio = st.text_input('Lote:')

        if lote_rastreio != "":
            rastreio_2 = load_uc_path(lote_rastreio)
            #st.dataframe(rastreio_2)
            AgGrid(rastreio_2,fit_columns_on_grid_load=True, theme='balham')

    with exp_transformacao:
        
        data = st.date_input('Data tranformação')
        result = load_paletes_transformados(data)

        download_xlsx_transformacao = downloadToExcel(result)
        st.download_button(label = 'Baixar tabela', data = download_xlsx_transformacao, file_name='dados_transformacao.xlsx')

        st.dataframe(result)

with tab_rec:##################################################################################################################
    rec, mov_rec = load_rec()
    exp_pos_rec = st.expander('Posição REC 👉', expanded=False)
    exp_mov_rec = st.expander('Movimentação REC 👉', expanded=False)

    with exp_pos_rec:
        st.dataframe(rec)

    with exp_mov_rec:
        #AgGrid(mov_rec, fit_columns_on_grid_load=True, theme='balham')
        download_xlsx_inv = downloadToExcel(mov_rec)
        st.download_button(label = 'Baixar tabela', data = download_xlsx_inv, file_name='dados_posicao_rec.xlsx')
        st.dataframe(mov_rec)

with tab_ydi:
    ydi = load_ydi()
    exp_pos_ydi = st.expander('Posição GR-YDI1 👉', expanded=False)
    
    with exp_pos_ydi:
        col1, col2 = st.columns(2)

        with col1:
            total_ucs = str(ydi.uc.count())
            st.text('Total UCs: '+total_ucs)
        with col2:
            total_peso = str(round(ydi.peso_de_carga.sum(),2))
            st.text('Total peso: '+total_peso)
        st.dataframe(ydi)

with tab_gr_prod:##################################################################################################################
    #st.experimental_data_editor(rep)
    exp_gr_prod = st.expander('GR-PROD 👉', expanded=False)
    exp_aap = st.expander('AAP-LOP3-SA1 👉', expanded=False)
    gr_prod, aap_lop3 = load_gr_prod()
    
    with exp_gr_prod:
        download_xlsx_gr_prod = downloadToExcel(gr_prod.sort_values('dias_posicao', ascending=False))
        st.download_button(label = 'Baixar tabela', data = download_xlsx_gr_prod, file_name='dados_gr_prod.xlsx')
        st.dataframe(gr_prod.sort_values('dias_posicao', ascending=False))

    with exp_aap:
        download_xlsx_aap = downloadToExcel(aap_lop3.sort_values('dias_posicao', ascending=False))
        st.download_button(label = 'Baixar tabela', data = download_xlsx_aap, file_name='dados_aap.xlsx')
        st.dataframe(aap_lop3.sort_values('dias_posicao', ascending=False))

with tab_rep:##################################################################################################################
    rep, mov_rep = load_rep()
    st.dataframe(rep)
    #AgGrid(rep, fit_columns_on_grid_load=True)
    st.dataframe(mov_rep)
    busca_uc_rep = st.text_input('UC rep:')
    if busca_uc_rep != '':
        tarefas_uc = busca_tarefas(busca_uc_rep)
        st.dataframe(tarefas_uc)

with tab_stock:##################################################################################################################
    
    exp_remessa = st.expander('Lotes por remessa 👉', expanded=False)
    exp_de_para = st.expander('Pesquisar DE x PARA 👉', expanded=False)
    exp_analise = st.expander('Análise de lotes 👉', expanded=False)

    with exp_remessa:

        opt_documento = st.radio('Tipo da busca', ['Remessa / Inbound','Nota fiscal'])

        if opt_documento == 'Remessa / Inbound':
            documento = st.text_input('Informe as remessas ou inbounds separadas por um espaço')
        elif opt_documento == 'Nota fiscal':
            documento = st.text_input('Informe as notas fiscais separadas por um espaço')

        if documento != "":
            documento = documento.split(' ')
            if opt_documento == 'Remessa / Inbound':            
                dados = load_remessas(documento)
            elif opt_documento == 'Nota fiscal':
                dados = load_remessas(documento, tipo='nota_fiscal')

            download_xlsx_dados = downloadToExcel(dados)
            st.download_button(label = 'Baixar tabela', data = download_xlsx_dados, file_name='dados_remessas.xlsx')
            st.write('Lotes encontrados: ' + str(dados.shape[0]))
            st.write('Peso total: ' + str(round(dados.quantidade.sum(),2)))
            st.dataframe(dados)

    with exp_de_para:
        material = st.text_input('Material:')

        if material != "":
            de_para = load_de_para(material.upper())
            st.dataframe(de_para)

    with exp_analise:
        df = st.file_uploader('Carregue o arquivo XLSX do estoque')

        txt_item = st.text_input('Informe um produto para analisar')
        txt_item = txt_item.upper()

        if txt_item != '':
            g = Cativa(df)
            ic_ideal = g.ic_ideal(txt_item)
            ic_real = g.ic_real(txt_item)

            st.write('IC Ideal: ' + str(ic_ideal))
            st.write('IC Real: ' + str(ic_real))

with tab_bot:

    exp_config = st.expander('Configurações atuais 👉', expanded=False)
    exp_execucoes = st.expander('Histórico de execuções 👉', expanded=False)

    ex = pd.read_json(r'C:\Users\matheus.leite\OneDrive - VALGROUP\From OneDrive\arquivos_bq_app\settings\controle\execucoes.json', orient='records')
    ex_bkp = pd.read_json(r'C:\Users\matheus.leite\OneDrive - VALGROUP\From OneDrive\arquivos_bq_app\settings\controle\execucoes_bkp.json', orient='records')
    
    f = open(r'C:\Users\matheus.leite\OneDrive - VALGROUP\From OneDrive\arquivos_bq_app\settings\settings.json')
    dados = json.load(f)
    f.close()

    horarios_fds = dados['horarios_fds']
    horarios = dados['horarios']
    processos = dados['processos']
    ot = dados['habilitar_ot']
    plantas = dados['plantas']
    plantas_app = dados['plantas_app']
    executar_bq = dados['executar_bq']

    with exp_config:
        st.write('Horários: ' + str(horarios))
        st.write('Horários fds: ' + str(horarios_fds))
        st.write('Processos: ' + str(processos))
        st.write('Ot: ' + str(ot))
        st.write('Plantas: ' + str(plantas))
        st.write('Plantas App: ' + str(plantas_app))
        st.write('Executar: ' + str(executar_bq))

    with exp_execucoes:
        st.write('Execuções BQ')
        st.dataframe(ex)
        st.write('Execuções BKP')
        st.dataframe(ex_bkp)
        #AgGrid(ex_bkp, fit_columns_on_grid_load=True)