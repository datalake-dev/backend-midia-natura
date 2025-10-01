import os
import json
import functions_framework
import google.generativeai as genai
from flask import jsonify
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "africa-br"
DATASET_ID = "NaturaProcessData"
TABLE_ID = "PreClique"
CAMPAIGN_FILTER = "tododia-havana"

genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
bigquery_client = bigquery.Client(project=PROJECT_ID)

ANALYSIS_PROMPT_TEMPLATE = """Você é um especialista sênior em análise de mídia paga da agência, encarregado de analisar a performance da campanha '{campaign}' para o cliente Natura.

**Sua Missão:**
Com base no resumo de dados da campanha fornecido abaixo (formato JSON), gere de 5 a 7 insights estratégicos e criativos sobre os resultados.

**Estrutura:**
- Use Markdown (negrito, listas numeradas).
- Liste os insights e finalize com uma recomendação estratégica geral.

**Dados da Campanha (JSON):**
{data_from_bq}
"""

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

analysis_model = genai.GenerativeModel('gemini-2.5-flash')

@functions_framework.http
def gemini_chat(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Credentials': 'true'
    }

    if request.method == 'OPTIONS':
        return ('', 204, headers)

    try:
        print("🚀 Iniciando análise...")
        print("→ Executando query no BigQuery...")

        query_job = bigquery_client.query(FIXED_SQL_QUERY)
        results = query_job.to_dataframe()

        print(f"✅ Query concluída. {len(results)} linhas retornadas.")

        if results.empty:
            print("⚠️ Nenhum resultado encontrado.")
            final_answer = f"Não encontrei dados para a campanha '{CAMPAIGN_FILTER}'."
            return jsonify({'text': final_answer}), 200, headers

        print("→ Formatando dados e enviando para o Gemini...")

        payload = results.to_dict(orient='records')
        data_as_string = json.dumps(payload[:200], ensure_ascii=False, indent=2)

        analysis_prompt = ANALYSIS_PROMPT_TEMPLATE.format(
            campaign=CAMPAIGN_FILTER,
            data_from_bq=data_as_string
        )

        print("→ Gerando resposta com Gemini...")
        final_response = analysis_model.generate_content(analysis_prompt)

        print("✅ Resposta gerada com sucesso pelo Gemini.")

        final_answer = final_response.text
        return jsonify({'text': final_answer}), 200, headers

    except Exception as e:
        print("❌ Ocorreu um erro:")
        import traceback
        traceback.print_exc()  # Mostra stacktrace completo nos logs
        final_answer = "Desculpe, ocorreu um erro ao consultar o banco de dados e gerar os insights."
        return jsonify({'text': final_answer}), 500, headers
