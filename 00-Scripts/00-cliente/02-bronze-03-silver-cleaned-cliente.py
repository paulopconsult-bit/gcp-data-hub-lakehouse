# ==============================================================================
# BIBLIOTECAS (As ferramentas da nossa faxina técnica)
# ==============================================================================
import pandas as pd            # Motor principal para transformação de dados
import os                      # Gestão de caminhos e variáveis de ambiente
import json                    # Para registrar o manifesto de limpeza
from datetime import datetime  # Para carimbos de tempo do processo

# ==============================================================================
# STEP 0: MAPEAMENTO E AMBIENTE
# ==============================================================================
# Abstração de ambiente: Local (G:) vs Docker (/app)
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\Docker\lakehouse")

# Definimos Origem (Bronze) e Destino (Silver Clean)
PATH_BRONZE    = os.path.join(BASE_PATH, "02-01-bronze", "00-cliente")
PATH_SILVER    = os.path.join(BASE_PATH, "03-silver-cleaned", "00-cliente")
PATH_MANIFESTO = os.path.join(BASE_PATH, "02-03-metadata-manifests", "00-cliente")

def gerar_manifesto_silver(qtd_entrada, qtd_saida, status):
    """Registra o sucesso da faxina no log centralizado (JSONL)"""
    registro = {
        "camada": "SILVER-CLEAN",
        "data_processamento": datetime.now().isoformat(),
        "linhas_lidas": qtd_entrada,
        "linhas_entregues": qtd_saida,
        "status": status
    }
    caminho_log = os.path.join(PATH_MANIFESTO, "log-execucao-cliente.jsonl")
    with open(caminho_log, 'a', encoding='utf-8') as f:
        f.write(json.dumps(registro) + "\n")

# ==============================================================================
# STEP 1: PROCESSO DE HIGIENIZAÇÃO (A VASSOURA TÉCNICA)
# Note que não há drop_duplicates. O que entrou na Bronze saiu na Silver Clean, 
# cumprindo o requisito de auditoria.
# ==============================================================================
def step_silver_clean():
    print(f"{'='*60}\n🚀 [STEP 02 -> 03] SILVER CLEAN: 00-CLIENTE\n{'='*60}")
    
    # --- 1.1: LEITURA DA BRONZE (NOSSA CAIXA PRETA) ---
    origem_parquet = os.path.join(PATH_BRONZE, "cliente.parquet")
    
    if not os.path.exists(origem_parquet):
        print("❌ Erro: Arquivo Bronze não encontrado para processar.")
        return

    df = pd.read_parquet(origem_parquet)
    qtd_inicial = len(df)
    print(f"📦 Bronze lida com sucesso: {qtd_inicial} registros.")

    try:
        # --- 1.2: TOKENIZAÇÃO DE QUEBRAS DE LINHA (MUNDO REAL) ---
        # Substituímos \n e \r por [NL] para não quebrar colunas em exportações futuras
        # Preservamos a intenção sem o efeito colateral técnico
        print("🧹 Higienizando caracteres de controle (\n, \r, \t)...")
        df = df.replace(r'\r+|\n+', ' [NL] ', regex=True)
        df = df.replace(r'\t+', ' ', regex=True) # Tabulação vira espaço simples

        # --- 1.3: TRIMMING E LIMPEZA DE TEXTO (STRINGS) ---
        # Removemos espaços inúteis no início e fim de cada célula de texto
        # Selecionamos apenas colunas que são 'object' (strings)
        cols_texto = df.select_dtypes(include=['object']).columns
        for col in cols_texto:
            df[col] = df[col].astype(str).str.strip()
            # Remove espaços duplos internos (limpeza fina)
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

        # --- 1.4: HIGIENIZAÇÃO DE TEXTO EM DATAS (ABORDAGEM CONSERVADORA) ---
        # Não forçamos o tipo 'datetime' para evitar a geração de nulos (NaT).
        # Mantemos o valor original intacto como 'object' (texto) para que a 
        # camada Enriched decida como tratar as variações de formato ou erros.
        print("📅 Higienizando strings de data (Sem conversão de tipo)...")
        df['data_nascimento'] = df['data_nascimento'].astype(str).str.strip()

        # --- 1.5: NORMALIZAÇÃO DE CASE (ESTÉTICA) ---
        # Estados em Maiúsculo (SP, RJ) e Nomes em Title Case (Primeira Maiúscula)
        if 'estado' in df.columns:
            df['estado'] = df['estado'].str.upper()
        if 'nome' in df.columns:
            df['nome'] = df['nome'].str.title()

        # --- 1.6: PERSISTÊNCIA NA SILVER CLEAN ---
        # Mantemos o formato Parquet e a granularidade total (mesmo número de linhas)
        os.makedirs(PATH_SILVER, exist_ok=True)
        destino_parquet = os.path.join(PATH_SILVER, "cliente.parquet")
        df.to_parquet(destino_parquet, index=False, engine='pyarrow')

        # --- 1.7: PROTOCOLO DE SUCESSO ---
        print(f"✅ Sucesso: {len(df)} linhas limpas e entregues na Silver Clean.")
        gerar_manifesto_silver(qtd_inicial, len(df), "SUCESSO")

    except Exception as e:
        print(f"❌ Erro na transformação Silver Clean: {e}")
        gerar_manifesto_silver(qtd_inicial, 0, f"ERRO: {str(e)}")

# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    step_silver_clean()