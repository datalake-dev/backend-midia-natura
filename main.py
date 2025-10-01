import os
import functions_framework
import google.generativeai as genai
from flask import make_response, jsonify
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
Com base no resumo de dados da campanha fornecido abaixo, sua tarefa é gerar de 5 a 7 insights estratégicos e criativos. Identifique os principais destaques (positivos e negativos), padrões e anomalias.

**Tom de Voz:**
- **Direto e Profissional:** Comunique-se de forma clara e objetiva, como um analista apresentando resultados para a equipe.
- **Evite Formalidades Excessivas:** Não use saudações como "Prezado(a)". Vá direto aos insights.
- **Foco em Ação:** Sua linguagem deve ser proativa, focada em apontar oportunidades e pontos de atenção.

**Estrutura da Resposta:**
- Use Markdown para formatação (negrito, listas, etc.).
- Organize os insights em uma lista numerada.
- Cada insight deve ser claro e vir acompanhado de uma breve justificativa baseada nos dados.
- Finalize com uma **"Recomendação Estratégica Geral"** com base em todos os seus achados.

**Dados da Campanha (Formato CSV):**
{data_from_bq}

**Gere sua análise agora.**
"""

# --- CONSULTA SQL FIXA E OTIMIZADA ---
# Esta consulta busca os dados agregados pelas principais dimensões para a análise.
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

# --- FUNÇÃO PRINCIPAL (AGORA MUITO MAIS SIMPLES) ---
@functions_framework.http
def gemini_chat(request):
    headers = {'Access-Control-Allow-Origin': '*','Access-Control-Allow-Methods': 'POST, OPTIONS','Access-Control-Allow-Headers': 'Content-Type'}
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    try:
        # ETAPA 1: Executar a consulta fixa no BigQuery
        query_job = bigquery_client.query(FIXED_SQL_QUERY)
        results = query_job.to_dataframe()
        
        if results.empty:
            final_answer = f"Não encontrei dados para a campanha '{CAMPAIGN_FILTER}'."
        else:
            # ETAPA 2: Enviar os dados para o Gemini gerar os insights
            data_as_string = results.to_csv(index=False)
            analysis_prompt = ANALYSIS_PROMPT_TEMPLATE.format(
                campaign=CAMPAIGN_FILTER,
                data_from_bq=data_as_string
            )
            final_response = analysis_model.generate_content(analysis_prompt)
            final_answer = final_response.text

        return jsonify({'text': final_answer})

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        final_answer = "Desculpe, ocorreu um erro ao consultar o banco de dados e gerar os insights."
        return jsonify({'text': final_answer}), 500
