import os
import functions_framework
import google.generativeai as genai
from flask import make_response
from google.cloud import bigquery
import pandas as pd

# --- CONFIGURAÇÕES DO SEU PROJETO (PREENCHIDO) ---
PROJECT_ID = "africa-br"
DATASET_ID = "NaturaProcessData"
TABLE_ID = "PreClique"

TABLE_SCHEMA_DESCRIPTION = """
(
    Submarca STRING, Campanha STRING, ObjetivodeComunicacao STRING, Plataforma STRING, FreeText1 STRING, ObjetivoDeMidia STRING, TipoAtivacao STRING, Estrategia STRING, Segmentacao STRING, FaixaEtaria STRING, Genero STRING, Praca STRING, Freetext2 STRING, CategoriaAd STRING, SubmarcaAd STRING, AdFront STRING, Formato STRING, Dimensao STRING, Segundagem STRING, Skippable STRING, Pilar STRING, AcaoComInfluenciador STRING, Influenciador STRING, RT1 STRING, LinhaCriativa STRING, CTA STRING, TipoDeCompra STRING, Canal STRING, PrecisionMkt STRING, ChaveGa STRING, Cost FLOAT64, Revenue FLOAT64, Impressions INT64, Clicks INT64, VideoViews INT64, ThreeSecondsVideoViews INT64, TwoSecondsVideoViews INT64, SixSecondsVideoViews INT64, VideoViews25 INT64, VideoViews50 FLOAT64, VideoViews75 INT64, VideoViews100 INT64, Comments INT64, Likes INT64, Saves INT64, Shares INT64, Follows INT64
)
"""
# --------------------------------------------------------------------

# Inicializa os clientes do Google
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
bigquery_client = bigquery.Client(project=PROJECT_ID)

# --- PERSONAS E PROMPTS ---
AGENT_PERSONA = """Você é um especialista sênior em análise de mídia paga para a Natura. Sua missão é analisar dados e responder perguntas de forma clara e acionável. Responda sempre em português do Brasil."""

SQL_GENERATOR_PROMPT = f"""Você é um expert em GoogleSQL (SQL do BigQuery). Sua única função é converter uma pergunta em linguagem natural em uma consulta SQL otimizada para uma tabela chamada `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`.

O esquema da tabela é:
{TABLE_SCHEMA_DESCRIPTION}

**Métricas Calculadas (Fórmulas):**
Quando o usuário pedir por uma métrica que não é uma coluna direta, use estas fórmulas para calculá-la. Use `SUM()` para agregar custos e contagens antes de dividir, para garantir a precisão dos cálculos.

- **CPM (Custo por Mil Impressões):** `(SUM(Cost) / SUM(Impressions)) * 1000`
- **CTR (Taxa de Cliques):** `(SUM(Clicks) / SUM(Impressions)) * 100`
- **CPC (Custo por Clique):** `SUM(Cost) / SUM(Clicks)`
- **CPView 100% ou CPV (Custo por View Completa):** `SUM(Cost) / SUM(VideoViews100)`
- **VTR 100% (Taxa de Visualização Completa):** `(SUM(VideoViews100) / SUM(Impressions)) * 100`

**Regras:**
1.  Responda APENAS com o código SQL. Não inclua explicações, markdown (` ```sql `) ou qualquer outro texto.
2.  Use a cláusula `WHERE` sempre que possível para filtrar os dados e reduzir o custo de processamento.
3.  **Busca Flexível:** Para filtrar por `Campanha`, sempre use `LOWER(Campanha) LIKE '%termo%'` para fazer buscas parciais que não diferenciam maiúsculas de minúsculas.
4.  A data de hoje é {pd.Timestamp.now().strftime('%Y-%m-%d')}. Use isso como referência para perguntas como "hoje", "ontem", "nesta semana".

Pergunta do usuário:
"""

# --- MODELOS GEMINI ---
sql_model = genai.GenerativeModel('gemini-2.5-flash')
analysis_model = genai.GenerativeModel(
    'gemini-2.5-flash',
    system_instruction=AGENT_PERSONA
)

# --- FUNÇÃO PRINCIPAL DO CHATBOT ---
@functions_framework.http
def gemini_chat(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    request_json = request.get_json(silent=True)
    if not request_json or 'prompt' not in request_json:
        return make_response(('Corpo da requisição inválido.', 400, headers))

    user_question = request_json['prompt']

    try:
        # ETAPA 1: Gerar a consulta SQL
        sql_generation_prompt = f"{SQL_GENERATOR_PROMPT}{user_question}"
        sql_response = sql_model.generate_content(sql_generation_prompt)
        sql_query = sql_response.text.strip().replace('`', '')

        # ETAPA 2: Executar a consulta no BigQuery
        query_job = bigquery_client.query(sql_query)
        results = query_job.to_dataframe()
        
        if results.empty:
            final_answer = "Não encontrei dados para a sua pergunta. Por favor, tente refinar sua busca ou verifique os filtros aplicados."
        else:
            # ETAPA 3: Enviar os dados para a persona analisar
            data_as_string = results.to_csv(index=False)
            analysis_prompt = (
                f"Com base nos seguintes dados extraídos do BigQuery em formato CSV:\n\n"
                f"{data_as_string}\n\n"
                f"Responda à pergunta original do usuário de forma clara e analítica: '{user_question}'"
            )
            final_response = analysis_model.generate_content(analysis_prompt)
            final_answer = final_response.text

        return make_response({'text': final_answer}, 200, headers)

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        error_message = str(e)
        if '400' in error_message and 'not found' in error_message.lower():
             final_answer = "Desculpe, a consulta gerada parece inválida. Talvez a pergunta se refira a colunas ou dados que não existem? Tente perguntar de outra forma."
        else:
             final_answer = "Desculpe, não consegui processar sua solicitação. Houve um erro ao consultar o banco de dados."
        
        return make_response(({'text': final_answer}), 500, headers)
