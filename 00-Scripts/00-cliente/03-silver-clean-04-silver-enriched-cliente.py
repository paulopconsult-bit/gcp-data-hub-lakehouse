# ==============================================================================
# BIBLIOTECAS (O motor de inteligência e transformação)
# ==============================================================================
import pandas as pd            # Transformação e tipagem de dados
import os                      # Mapeamento de ambiente (ROOT_PROJETO)
import json                    # Registro de manifesto de qualidade
from datetime import datetime  # Cálculos de idade e carimbos de tempo

# ==============================================================================
# STEP 0: ABSTRAÇÃO DE AMBIENTE E MAPEAMENTO
# ==============================================================================
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\Docker\lakehouse")

PATH_CLEAN     = os.path.join(BASE_PATH, "03-silver-cleaned", "00-cliente")
PATH_ENRICHED  = os.path.join(BASE_PATH, "04-silver-enriched", "00-cliente")
PATH_MANIFESTO = os.path.join(BASE_PATH, "02-03-metadata-manifests", "00-cliente")

def gerar_manifesto_enriched(lidas, entregues, status):
    """Manifesto focado em Qualidade de Dados (Data Quality)"""
    registro = {
        "camada": "SILVER-ENRICHED",
        "data_processamento": datetime.now().isoformat(),
        "linhas_origem": lidas,
        "linhas_finais": entregues,
        "duplicatas_removidas": lidas - entregues,
        "status": status
    }
    caminho_log = os.path.join(PATH_MANIFESTO, "log-execucao-cliente.jsonl")
    with open(caminho_log, 'a', encoding='utf-8') as f:
        f.write(json.dumps(registro) + "\n")

# ==============================================================================
# STEP 1: JURISDIÇÃO DE NEGÓCIO E TIPAGEM FORTE
# ==============================================================================
def step_silver_enriched():
    print(f"{'='*60}\n🚀 [STEP 03 -> 04] SILVER ENRICHED: tb_cliente\n{'='*60}")
    
    origem = os.path.join(PATH_CLEAN, "cliente.parquet")
    if not os.path.exists(origem):
        print("❌ Origem Clean não encontrada.")
        return

    df = pd.read_parquet(origem)
    qtd_lidas = len(df)

    try:
        # --- 1.1: DEDUPLICAÇÃO (REGRA DE NEGÓCIO: SNAPSHOT ATUAL) ---
        # Decisão técnica: O registro mais recente ('last') é a nossa verdade atualizada.
        # O histórico completo permanece seguro na camada Bronze.
        print("🧬 Removendo duplicatas (Mantendo a versão mais recente)...")
        df = df.sort_values('_at_captura').drop_duplicates(subset=['id_cliente'], keep='last')

        # --- 1.2: NORMALIZAÇÃO DE CHAVE (ID DE ELITE) ---
        # IDs são tratados como texto para evitar perda de zeros e erros de modelagem.
        # Padrão: Prefixo 'c' + 5 dígitos (ex: c00001)
        print("🔑 Aplicando máscara de ID (Ex: c00001)...")
        df['id_cliente'] = 'c' + df['id_cliente'].astype(str).str.replace('.0', '', regex=False).str.zfill(5)

        # --- 1.3: TRADUÇÃO DE NOMENCLATURA (SNAKE_CASE) ---
        # Padronização semântica para facilitar o consumo em DW e Power BI.
        print("🏷️ Renomeando colunas para padrão tb_cliente...")
        dicionario_nomes = {
            'nome': 'nm_cliente',
            'estado': 'uf',
            'data_nascimento': 'dt_nascimento'
        }
        df = df.rename(columns=dicionario_nomes)

        # --- 1.4: TIPAGEM E GESTÃO DE DATAS (SEM AUTO-AJUSTE) ---
        # Tentamos a conversão. Se for lixo, vira Vazio (NaT). 
        # O negócio deverá ver o vazio e cobrar a correção da fonte.
        print("📅 Convertendo dt_nascimento para tipo DATE...")
        df['dt_nascimento'] = pd.to_datetime(df['dt_nascimento'], errors='coerce').dt.date

        # --- 1.5: ENRIQUECIMENTO (CÁLCULO DE IDADE) ---
        # Valor agregado direto para o Analytics
        hoje = datetime.now()
        df['nr_idade'] = df['dt_nascimento'].apply(
            lambda x: hoje.year - x.year - ((hoje.month, hoje.day) < (x.month, x.day)) 
            if pd.notnull(x) else None
        )

        # --- 1.6: PERSISTÊNCIA FINAL (TB_CLIENTE) ---
        os.makedirs(PATH_ENRICHED, exist_ok=True)
        df.to_parquet(os.path.join(PATH_ENRICHED, "tb_cliente.parquet"), index=False)

        print(f"✅ Sucesso: {len(df)} registros únicos entregues na Silver Enriched.")
        gerar_manifesto_enriched(qtd_lidas, len(df), "SUCESSO")

    except Exception as e:
        print(f"❌ Erro na Enriched: {e}")
        gerar_manifesto_enriched(qtd_lidas, 0, f"ERRO: {str(e)}")

if __name__ == "__main__":
    step_silver_enriched()