import os
import json
import functions_framework
import google.generativeai as genai
from flask import jsonify
from google.cloud import bigquery
import pandas as pd

# --- CONFIGURAÇÕES DO PROJETO ---
PROJECT_ID = "africa-br"
DATASET_ID = "NaturaProcessData"
TABLE_ID = "PreClique"
CAMPAIGN_FILTER = "tododia-havana"

# Inicializa os clientes
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
bigquery_client = bigquery.Client(project=PROJECT_ID)

# --- PROMPT DE ANÁLISE PROATIVA ---
ANALYSIS_PROMPT_TEMPLATE = """Você é um especialista sênior em análise de mídia paga da agência, encarregado de analisar a performance da campanha '{campaign}' para o cliente Natura.

**Sua Missão:**
Com base no resumo de dados da campanha fornecido abaixo (formato JSON), sua tarefa é gerar de 5 a 7 insights estratégicos e criativos. Identifique os principais destaques (positivos e negativos), padrões e anomalias.

**Tom de Voz:**
- **Direto e Profissional:** Comunique-se de forma clara e objetiva.
- **Evite Formalidades Excessivas:** Vá direto aos insights.
- **Foco em Ação:** Apresente recomendações práticas.

**Estrutura da Resposta:**
- Use Markdown (negrito, listas numeradas).
- Organize os insights em uma lista numerada.
- Cada insight deve ter uma breve justificativa baseada nos dados.
- Finalize com uma **"Recomendação Estratégica Geral"**.

**Dados da Campanha (JSON):**
{data_from_bq}

**Gere sua análise agora.**
"""

# --- CONSULTA SQL FIXA E OTIMIZADA ---
FIXED_SQL_QUERY = f"""
SELECT
    Plataforma,
    Segmentacao,
    LinhaCriativa,
    Formato,
    SUM(Cost) as CustoTotal,
    SUM(Impressions) as Impressoes,
    SUM(Clicks) as Cliques,
    SUM(VideoViews100) as ViewsCompletas
FROM
    `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
WHERE
    LOWER(Campanha) LIKE '%{CAMPAIGN_FILTER}%'
GROUP BY
    Plataforma, Segmentacao, LinhaCriativa, Formato
"""

# --- MODELO GEMINI ---
analysis_model = genai.GenerativeModel('gemini-2.5-flash')

# --- FUNÇÃO PRINCIPAL ---
@functions_framework.http
def gemini_chat(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Credentials': 'true'
    }

    # Pré-flight CORS
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    try:
        # Executa a query
        query_job = bigquery_client.query(FIXED_SQL_QUERY)
        results = query_job.to_dataframe()

        if results.empty:
            final_answer = f"Não encontrei dados para a campanha '{CAMPAIGN_FILTER}'."
            return jsonify({'text': final_answer}), 200, headers

        # Constrói payload estruturado (schema + rows). Limita número de linhas.
        rows = results.to_dict(orient='records')
        schema = list(results.columns)
        max_rows = 200
        payload = {
            'schema': schema,
            'rows': rows[:max_rows],
            'total_rows_in_query': len(rows)
        }

        data_as_string = json.dumps(payload, ensure_ascii=False, indent=2)

        analysis_prompt = ANALYSIS_PROMPT_TEMPLATE.format(
            campaign=CAMPAIGN_FILTER,
            data_from_bq=data_as_string
        )

        final_response = analysis_model.generate_content(analysis_prompt)
        final_answer = final_response.text

        return jsonify({'text': final_answer}), 200, headers

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        final_answer = "Desculpe, ocorreu um erro ao consultar o banco de dados e gerar os insights."
        return jsonify({'text': final_answer}), 500, headers
