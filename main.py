import os
import re
import functions_framework
import google.generativeai as genai
from flask import make_response, jsonify
from google.cloud import bigquery
import pandas as pd

# --- CONFIGURAÇÕES DO PROJETO ---
PROJECT_ID = "africa-br"
DATASET_ID = "NaturaProcessData"
TABLE_ID = "PreClique"

TABLE_SCHEMA_DESCRIPTION = """
(
    Submarca STRING, Campanha STRING, ObjetivodeComunicacao STRING, Plataforma STRING, FreeText1 STRING, ObjetivoDeMidia STRING, TipoAtivacao STRING, Estrategia STRING, Segmentacao STRING, FaixaEtaria STRING, Genero STRING, Praca STRING, Freetext2 STRING, CategoriaAd STRING, SubmarcaAd STRING, AdFront STRING, Formato STRING, Dimensao STRING, Segundagem STRING, Skippable STRING, Pilar STRING, AcaoComInfluenciador STRING, Influenciador STRING, RT1 STRING, LinhaCriativa STRING, CTA STRING, TipoDeCompra STRING, Canal STRING, PrecisionMkt STRING, ChaveGa STRING, Cost FLOAT64, Revenue FLOAT64, Impressions INT64, Clicks INT64, VideoViews INT64, ThreeSecondsVideoViews INT64, TwoSecondsVideoViews INT64, SixSecondsVideoViews INT64, VideoViews25 INT64, VideoViews50 FLOAT64, VideoViews75 INT64, VideoViews100 INT64, Comments INT64, Likes INT64, Saves INT64, Shares INT64, Follows INT64
)
"""

# Inicializa os clientes
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
bigquery_client = bigquery.Client(project=PROJECT_ID)

# --- PERSONAS E PROMPTS ---
AGENT_PERSONA = """Você é um especialista sênior em análise de mídia paga da agência, focado exclusivamente na campanha 'Tododia Havana' para o cliente Natura. Sua missão é gerar insights claros, concisos e acionáveis em formato Markdown.

**Tom de Voz:**
- **Direto e Profissional:** Comunique-se de forma clara e objetiva.
- **Evite Formalidades Excessivas:** Não use saudações como "Prezado(a)". Vá direto ao insight.
- **Foco em Ação:** Sua linguagem deve ser proativa, focada em apontar oportunidades e pontos de atenção.

**Estrutura da Resposta:**
1. Comece com a conclusão ou insight principal em negrito.
2. Apresente os dados de apoio, preferencialmente em uma tabela simples.
3. Finalize com uma **"Recomendação:"** ou **"Ponto de Atenção:"**.

Responda sempre em português do Brasil.
"""

SQL_GENERATOR_PROMPT = f"""Você é um expert em GoogleSQL (SQL do BigQuery). Sua única função é converter uma pergunta em linguagem natural em uma consulta SQL otimizada para uma tabela chamada `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`.

O esquema da tabela é:
{TABLE_SCHEMA_DESCRIPTION}

**Regra de Ouro:** Use **EXCLUSIVAMENTE** os nomes de colunas fornecidos no esquema acima. Não infira ou utilize qualquer outro nome de coluna ou pseudo-coluna.

**Filtro Fixo de Campanha:** Todas as consultas que você gerar DEVEM, obrigatoriamente, incluir o filtro `LOWER(Campanha) LIKE '%tododia-havana%'`. Se o usuário solicitar outros filtros (por plataforma, segmentação, etc.), adicione-os usando a cláusula `AND`.

**Métricas Calculadas (Fórmulas):**
- **CPM:** `(SUM(Cost) / SUM(Impressions)) * 1000`
- **CTR:** `(SUM(Clicks) / SUM(Impressions)) * 100`
- **CPC:** `(SUM(Cost) / SUM(Clicks))`
- **CPView 100% ou CPV:** `(SUM(Cost) / SUM(VideoViews100))`
- **VTR 100%:** `(SUM(VideoViews100) / SUM(Impressions)) * 100`

**Regras de Geração:**
1.  **Análise para Otimização:** Se a pergunta for sobre otimização ou comparação, retorne a métrica principal AGRUPADA pelas dimensões mais relevantes.
2.  **Análise de Tendência:** Se a pergunta envolver "tendência" ou "evolução", agrupe a métrica por `DATE_TRUNC(data, MONTH)`. A coluna de data se chama `data`.
3.  Responda APENAS com o código SQL.
4.  **Filtros Flexíveis e Sinônimos:** Aplique filtros flexíveis (`LOWER(coluna) LIKE '%termo%'`) para: `Plataforma`, `Segmentacao` (sinônimos: audiência), `Canal`, `ObjetivoDeMidia`, `ObjetivodeComunicacao`, `PrecisionMkt` (sinônimos: precision, pm). Note que o filtro de `Campanha` já está fixo.
5.  A data de hoje é {pd.Timestamp.now().strftime('%Y-%m-%d')}.

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
    # (O restante da função continua exatamente o mesmo das versões anteriores)
    headers = {'Access-Control-Allow-Origin': '*','Access-Control-Allow-Methods': 'POST, OPTIONS','Access-Control-Allow-Headers': 'Content-Type'}
    if request.method == 'OPTIONS':
        return ('', 204, headers)
    # ... (restante da função sem alterações)
