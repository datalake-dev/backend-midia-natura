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

# --- PROMPT DE ANÁLISE PROATIVA (AJUSTADO PARA CSV) ---
ANALYSIS_PROMPT_TEMPLATE = """Você é um especialista sênior em análise de mídia paga da agência, encarregado de analisar a performance da campanha '{campaign}' para o cliente Natura.

**Sua Missão:**
Com base em uma amostra representativa dos dados da campanha fornecida abaixo (formato CSV), sua tarefa é gerar de 5 a 7 insights estratégicos e criativos. Identifique os principais destaques, padrões e anomalias.

**Tom de Voz e Estrutura:**
- Vá direto ao primeiro insight, sem introduções.
- Use Markdown e finalize com uma Recomendação Estratégica Geral.
- Leve em conta o contexto de negócio da coluna 'PrecisionMkt' (PM = otimizável, NPM = não otimizável).

**Dados da Campanha (Amostra em CSV):**
{data_from_bq}

**Gere sua análise agora, começando pelo Insight 1.**
"""

# (A CONSULTA SQL FIXA CONTINUA A MESMA)
FIXED_SQL_QUERY = f"""
SELECT
    Plataforma, Segmentacao, LinhaCriativa, Formato, PrecisionMkt,
    SUM(Cost) as CustoTotal, SUM(Impressions) as Impressoes,
    SUM(Clicks) as Cliques, SUM(VideoViews100) as ViewsCompletas
FROM
    `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
WHERE
    LOWER(Campanha) LIKE '%{CAMPAIGN_FILTER}%'
    AND LOWER(Canal) NOT LIKE '%deal%'
GROUP BY
    Plataforma, Segmentacao, LinhaCriativa, Formato, PrecisionMkt
"""

# --- MODELO GEMINI ---
analysis_model = genai.GenerativeModel('gemini-2.5-flash')

# --- FUNÇÃO PRINCIPAL ---
@functions_framework.http
def gemini_chat(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
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
            final_answer = f"Não encontrei dados otimizáveis para a campanha '{CAMPAIGN_FILTER}'."
            return jsonify({'text': final_answer}), 200, headers

        print("→ Formatando dados (amostragem CSV) e enviando para o Gemini...")

        # --- OTIMIZAÇÃO APLICADA AQUI ---
        # Pega uma amostra aleatória de até 50 linhas para manter o prompt ágil
        sample_size = min(50, len(results))
        sample_df = results.sample(n=sample_size)
        data_as_string = sample_df.to_csv(index=False)
        # --- FIM DA OTIMIZAÇÃO ---

        analysis_prompt = ANALYSIS_PROMPT_TEMPLATE.format(
            campaign=CAMPAIGN_FILTER,
            data_from_bq=data_as_string
        )

        print("→ Gerando resposta com Gemini...")
        final_response = analysis_model.generate_content(analysis_prompt)
        final_answer = final_response.text

        print("✅ Resposta gerada com sucesso pelo Gemini.")
        return jsonify({'text': final_answer}), 200, headers

    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")
        import traceback
        traceback.print_exc()
        final_answer = "Desculpe, ocorreu um erro ao consultar o banco de dados e gerar os insights."
        return jsonify({'text': final_answer}), 500, headers
