import os
import json
import functions_framework
import google.generativeai as genai
from flask import jsonify
from google.cloud import bigquery
import pandas as pd
import traceback

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
Com base no resumo de dados da campanha fornecido abaixo (formato JSON), gere de 5 a 7 insights estratégicos e criativos sobre os resultados.

**Contexto de Negócio Importante:**
- A coluna `PrecisionMkt` indica se a linha é otimizável. 'PM' (Precision Marketing) significa que a compra é de leilão e pode ter seus lances otimizados. 'NPM' (Non-Precision Marketing) significa que a compra é de reserva (ex: TopView do TikTok) e não é otimizável via leilão. Suas recomendações devem levar essa diferença fundamental em consideração.

**Tom de Voz:**
- **Direto e Profissional:** Comunique-se de forma clara e objetiva, como um analista apresentando resultados para a equipe.
- **Evite Formalidades Excessivas:** Não use saudações como "Prezado(a)".

**Estrutura da Resposta:**
- **Comece a resposta DIRETAMENTE pelo primeiro insight (ex: "1. Destaque sobre...").** Não use nenhuma introdução ou parágrafo de apresentação.
- Use Markdown para formatação (negrito, listas numeradas).
- Organize os insights em uma lista numerada.
- Finalize com uma **"Recomendação Estratégica Geral"**.

**Dados da Campanha (JSON):**
{data_from_bq}

**Gere sua análise agora, começando pelo Insight 1.**
"""

# --- CONSULTA SQL FIXA E OTIMIZADA ---
FIXED_SQL_QUERY = f"""
SELECT
    Plataforma,
    Segmentacao,
    LinhaCriativa,
    Formato,
    PrecisionMkt,
    SUM(Cost) as CustoTotal,
    SUM(Impressions) as Impressoes,
    SUM(Clicks) as Cliques,
    SUM(VideoViews100) as ViewsCompletas
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
            print("⚠️ Nenhum resultado encontrado (após filtro de 'deal').")
            final_answer = f"Não encontrei dados otimizáveis para a campanha '{CAMPAIGN_FILTER}' (canais do tipo 'deal' foram excluídos da análise)."
            return jsonify({'text': final_answer}), 200, headers

        print("→ Formatando dados e enviando para o Gemini...")

        rows = results.to_dict(orient='records')
        # CORREÇÃO 1: JSON mais compacto, sem indentação
        data_as_string = json.dumps(rows[:200], ensure_ascii=False)

        analysis_prompt = ANALYSIS_PROMPT_TEMPLATE.format(
            campaign=CAMPAIGN_FILTER,
            data_from_bq=data_as_string
        )

        print("→ Gerando resposta com Gemini (com timeout estendido)...")
        
        # CORREÇÃO 2: Aumenta o tempo limite da chamada para 300 segundos (5 minutos)
        request_options = {"timeout": 300}
        final_response = analysis_model.generate_content(analysis_prompt, request_options=request_options)

        print("✅ Resposta gerada com sucesso pelo Gemini.")

        final_answer = final_response.text
        return jsonify({'text': final_answer}), 200, headers

    except Exception as e:
        print("❌ Ocorreu um erro:")
        traceback.print_exc()
        final_answer = "Desculpe, ocorreu um erro ao consultar o banco de dados e gerar os insights."
        return jsonify({'text': final_answer}), 500, headers
