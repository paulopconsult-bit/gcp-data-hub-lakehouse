# ==============================================================================
# BIBLIOTECAS (As ferramentas do nosso robô)
# ==============================================================================
import pandas as pd            # Manipulação de tabelas e arquivos Parquet
import uuid                    # Geração do _id_registro único por linha
import json                    # Leitura do contrato e escrita do manifesto
import os                      # Navegação no sistema e captura de metadados (mtime)
import glob                    # Busca de arquivos por padrão (Coringa *)
import pyarrow                 # Motor para o formato Parquet
import shutil                  # Movimentação física de arquivos (Mover/Higienizar)
from datetime import datetime  # Gestão de carimbos de tempo

# ==============================================================================
# STEP 0: ABSTRAÇÃO DE AMBIENTE E MAPEAMENTO
# ==============================================================================
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\Docker\lakehouse")

PATH_RAW       = os.path.join(BASE_PATH, "01-00-raw", "00-cliente")
PATH_BACKUP    = os.path.join(BASE_PATH, "02-00-raw-backup", "00-cliente")
PATH_BRONZE    = os.path.join(BASE_PATH, "02-01-bronze", "00-cliente")
PATH_ERRO      = os.path.join(BASE_PATH, "02-02-quarantine-erros", "00-cliente")
PATH_MANIFESTO = os.path.join(BASE_PATH, "02-03-metadata-manifests", "00-cliente")
PATH_CONTRATO  = os.path.join(BASE_PATH, "77-schema-controls", "00-cliente", "schema-controls.json")

def carregar_contrato():
    """Busca o 'contrato' de colunas na pasta de governança (77)"""
    with open(PATH_CONTRATO, 'r') as f:
        return json.load(f)

def gerar_manifesto(nome_arquivo, status, qtd_linhas, arquivo_controle, dt_origem="N/A", erro=None):
    """Gera um log acumulativo (JSON Lines) para auditoria centralizada"""
    registro = {
        "arquivo_original": nome_arquivo,
        "arquivo_controle": arquivo_controle,  # Link para o arquivo na pasta de Backup ou Erro
        "data_origem_fonte": dt_origem,
        "status": status,
        "data_processamento": datetime.now().isoformat(),
        "linhas_novas": qtd_linhas,
        "erro_detalhe": str(erro) if erro else "Nenhum"
    }

    # Caminho do log único (acumulativo)
    caminho_log = os.path.join(PATH_MANIFESTO, "log-execucao-cliente.jsonl")
    os.makedirs(PATH_MANIFESTO, exist_ok=True)

    # Abre em modo 'a' (append) para adicionar linhas sem apagar o passado
    with open(caminho_log, 'a', encoding='utf-8') as f:
        f.write(json.dumps(registro) + "\n")
    
    print(f"📒 Log registrado no histórico: {status}")

# ==============================================================================
# STEP 1: EXECUÇÃO DO FLUXO BRONZE (MODO ESTEIRA / PURISTA)
# ==============================================================================
def step_bronze():
    print(f"{'='*60}\n🚀 [MODO ESTEIRA] BRONZE PURISTA: 00-CLIENTE\n{'='*60}")
    
    # 1.1 - SETUP DE VARREDURA (ACORDO COM A FONTE)
    # Acordo: Qualquer CSV que termine obrigatoriamente com 'cliente.csv'
    padrao_busca = os.path.join(PATH_RAW, "*cliente.csv")
    arquivos_encontrados = glob.glob(padrao_busca)
    
    if not arquivos_encontrados:
        print("✨ RAW higienizada. Sem novos arquivos para processar.")
        return

    contrato = carregar_contrato()
    colunas_esperadas = contrato['colunas_esperadas']

    # 1.2 - O LOOP DA ESTEIRA (Processa um por um de forma independente)
    for caminho_completo in arquivos_encontrados:
        arquivo_nome = os.path.basename(caminho_completo)
        
        # --- [BLOQUEIO DE SEGURANÇA] ---
        # Capturamos a data do sistema operacional ANTES de entrar no try.
        # Assim, mesmo que o arquivo esteja corrompido, sabemos quando ele foi criado.
        mtime_timestamp = os.path.getmtime(caminho_completo)
        dt_modificacao_origem = datetime.fromtimestamp(mtime_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n📦 Processando: {arquivo_nome}")
        
        try:
            # 1.4 - LEITURA E VALIDAÇÃO DE SCHEMA
            df_novo = pd.read_csv(caminho_completo)
            if list(df_novo.columns) != colunas_esperadas:
                raise ValueError(f"Schema violado! Esperado: {colunas_esperadas}")

            # 1.5 - ENRIQUECIMENTO (COLUNAS DE INTELIGÊNCIA)
            df_novo['_id_registro'] = [str(uuid.uuid4()) for _ in range(len(df_novo))]
            df_novo['_at_captura'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_novo['_arquivo_origem'] = arquivo_nome
            df_novo['_arquivo_dt_modificacao'] = dt_modificacao_origem

            # 1.6 - LÓGICA DE ACUMULAÇÃO (APPEND PURO / COMPLIANCE)
            destino_bronze = os.path.join(PATH_BRONZE, "cliente.parquet")
            os.makedirs(PATH_BRONZE, exist_ok=True)

            if os.path.exists(destino_bronze):
                df_antigo = pd.read_parquet(destino_bronze)
                df_final = pd.concat([df_antigo, df_novo], ignore_index=True)
            else:
                df_final = df_novo

            df_final.to_parquet(destino_bronze, index=False, engine='pyarrow')

            # 1.7 - PERSISTÊNCIA E HIGIENIZAÇÃO (SUCESSO)
            os.makedirs(PATH_BACKUP, exist_ok=True)
            data_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
            nome_controle_bk = f"{data_str}-{arquivo_nome}" 
            destino_backup = os.path.join(PATH_BACKUP, nome_controle_bk)
            shutil.move(caminho_completo, destino_backup)

            print(f"✅ Sucesso integrado: {arquivo_nome}")
            gerar_manifesto(arquivo_nome, "SUCESSO", len(df_novo), nome_controle_bk, dt_modificacao_origem)

        except Exception as e:
            # 1.8 - ISOLAMENTO EM CASO DE ERRO (QUARENTENA)
            print(f"❌ ANOMALIA em {arquivo_nome}: {e}")
            os.makedirs(PATH_ERRO, exist_ok=True)
            data_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
            nome_controle_err = f"{data_str}-{arquivo_nome}" 
            destino_erro = os.path.join(PATH_ERRO, nome_controle_err)
            
            if os.path.exists(caminho_completo):
                shutil.move(caminho_completo, destino_erro)
                print(f"⚠️ Arquivo isolado na QUARENTENA.")
            
            # AQUI ESTÁ O PULO DO GATO: dt_modificacao_origem agora existe no escopo do erro!
            gerar_manifesto(arquivo_nome, "ERRO", 0, nome_controle_err, dt_modificacao_origem, erro=e)
if __name__ == "__main__":
    step_bronze()