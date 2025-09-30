import os
import re
import functions_framework
import google.generativeai as genai
from flask import make_response, jsonify
from google.cloud import bigquery
import pandas as pd

# --- CONFIGURAÇÕES DO PROJETO (PREENCHIDO) ---
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
AGENT_PERSONA = """Você é um especialista sênior em análise de mídia paga para a Natura. Sua missão é analisar dados e responder perguntas de forma clara, acionável e visualmente organizada.

**Regra de Formatação:** Sempre formate suas respostas usando Markdown para melhor clareza. Use negrito (`**texto**`) para destacar métricas e organize dados comparativos em tabelas simples. Responda sempre em português do Brasil."""

SQL_GENERATOR_PROMPT = f"""Você é um expert em GoogleSQL (SQL do BigQuery). Sua única função é converter uma pergunta em linguagem natural em uma consulta SQL otimizada para uma tabela chamada `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`.

O esquema da tabela é:
{TABLE_SCHEMA_DESCRIPTION}

--- INÍCIO DA ATUALIZAÇÃO ---
**Regra de Ouro:** Use **EXCLUSIVAMENTE** os nomes de colunas fornecidos no esquema acima. Não infira ou utilize qualquer outro nome de coluna ou pseudo-coluna (como `_PARTITIONTIME`). Todos os filtros e seleções devem se referir a colunas existentes no esquema.
--- FIM DA ATUALIZAÇÃO ---

**Métricas Calculadas (Fórmulas):**
Quando o usuário pedir por uma métrica que não é uma coluna direta, use estas fórmulas para calculá-la. Use `SUM()` para agregar custos e contagens.

- **CPM:** `(SUM(Cost) / SUM(Impressions)) * 1000`
- **CTR:** `(SUM(Clicks) / SUM(Impressions)) * 100`
- **CPC:** `SUM(Cost) / SUM(Clicks)`
- **CPView 100% ou CPV:** `SUM(Cost) / SUM(VideoViews100)`
- **VTR 100%:** `(SUM(VideoViews100) / SUM(Impressions)) * 100`

**Regras de Geração:**
1.  **Análise para Otimização:** Se a pergunta for sobre otimização ou comparação, retorne a métrica principal AGRUPADA pelas dimensões mais relevantes.
2.  **Análise de Tendência:** Se a pergunta envolver "tendência" ou "evolução", agrupe a métrica por `DATE_TRUNC(data, MONTH)` (para mês) ou `data` (para dia), e ordene pela data. A coluna de data se chama `data`.
3.  Responda APENAS com o código SQL.
4.  Use a cláusula `WHERE` sempre que possível.
5.  **Filtros Flexíveis e Sinônimos:** Aplique filtros flexíveis (`LOWER(coluna) LIKE '%termo%'`) para: `Campanha`, `Plataforma`, `Segmentacao` (sinônimos: audiência), `Canal`, `ObjetivoDeMidia`, `ObjetivodeComunicacao`, `PrecisionMkt` (sinônimos: precision, pm).
6.  A data de hoje é {pd.Timestamp.now().strftime('%Y-%m-%d')}.

Pergunta do usuário:
"""

# --- MODELOS GEMINI ---
# (O resto do código continua exatamente o mesmo)
sql_model = genai.GenerativeModel('gemini-2.5-flash')
analysis_model = genai.GenerativeModel(
    'gemini-2.5-flash',
    system_instruction=AGENT_PERSONA
)

# --- FUNÇÃO PRINCIPAL DO CHATBOT ---
@functions_framework.http
def gemini_chat(request):
    headers = {'Access-Control-Allow-Origin': '*','Access-Control-Allow-Methods': 'POST, OPTIONS','Access-Control-Allow-Headers': 'Content-Type'}
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    request_json = request.get_json(silent=True)
    if not request_json or 'prompt' not in request_json:
        return make_response(('Corpo da requisição inválido.', 400, headers))

    user_question = request_json['prompt']
    history = request_json.get('history', [])

    try:
        sql_chat_session = sql_model.start_chat(history=history[:-1])
        sql_generation_prompt = f"{SQL_GENERATOR_PROMPT}{user_question}"
        sql_response = sql_chat_session.send_message(sql_generation_prompt)
        sql_query = sql_response.text.strip().replace('`', '')

        query_job = bigquery_client.query(sql_query)
        results = query_job.to_dataframe()
        
        if results.empty:
            final_answer = "Não encontrei dados para a sua pergunta. Por favor, tente refinar sua busca ou verifique os filtros aplicados."
            suggestions = ["Qual o investimento total no último mês?", "Compare o CTR de todas as plataformas."]
        else:
            data_as_string = results.to_csv(index=False)
            analysis_prompt = (f"Com base nos seguintes dados extraídos do BigQuery em formato CSV:\n\n{data_as_string}\n\nResponda à pergunta original do usuário de forma clara, analítica e proativa, sugerindo otimizações: '{user_question}'.\n\n---FIM DA ANÁLISE---\nAgora, adicione o separador `###SUGESTÕES###` e, em uma nova linha, sugira 2 ou 3 perguntas de acompanhamento relevantes que um analista faria a seguir. Não adicione nenhum outro texto após as sugestões.")
            final_response = analysis_model.generate_content(analysis_prompt)
            
            if '###SUGESTÕES###' in final_response.text:
                parts = final_response.text.split('###SUGESTÕES###')
                final_answer = parts[0].strip()
                suggestions = re.findall(r'[\d\.\-\*\s]*([A-Z][^?]*\?)', parts[1])
                suggestions = [s.strip() for s in suggestions]
            else:
                final_answer = final_response.text.strip()
                suggestions = []

        return jsonify({'text': final_answer, 'suggestions': suggestions})

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        error_message = str(e)
        if '400' in error_message and 'not found' in error_message.lower():
             final_answer = "Desculpe, a consulta gerada parece inválida. Talvez a pergunta se refira a colunas ou dados que não existem?"
        else:
             final_answer = "Desculpe, não consegui processar sua solicitação. Houve um erro ao consultar o banco de dados."
        
        return jsonify({'text': final_answer, 'suggestions': ["Qual o investimento total?", "Liste as 5 principais campanhas por custo." ]}), 500
