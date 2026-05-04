# ==============================================================================
# BIBLIOTECAS (O carimbo final do processo)
# ==============================================================================
import pandas as pd            # Persistência final
import os                      # Mapeamento de ambiente (ROOT_PROJETO)
import json                    # Registro do manifesto de entrega (SLA)
from datetime import datetime  # Data de oficialização da carga

# ==============================================================================
# STEP 0: ABSTRAÇÃO DE AMBIENTE E MAPEAMENTO
# ==============================================================================
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\Docker\lakehouse")
PATH_ENRICHED  = os.path.join(BASE_PATH, "04-silver-enriched", "00-cliente")
PATH_GOLD      = os.path.join(BASE_PATH, "05-gold-business", "00-cliente")
PATH_MANIFESTO = os.path.join(BASE_PATH, "02-03-metadata-manifests", "00-cliente")

def gerar_manifesto_gold(qtd_final, status):
    """Manifesto de Entrega de Valor (SLA de Disponibilidade)"""
    registro = {
        "camada": "GOLD-BUSINESS",
        "tabela": "tb_cliente",
        "data_entrega": datetime.now().isoformat(),
        "total_registros_disponiveis": qtd_final,
        "status": status,
        "mensagem": "Dado oficializado para consumo (BI/Looker/PowerBI)"
    }
    caminho_log = os.path.join(PATH_MANIFESTO, "log-execucao-cliente.jsonl")
    with open(caminho_log, 'a', encoding='utf-8') as f:
        f.write(json.dumps(registro) + "\n")

# ==============================================================================
# STEP 1: OFICIALIZAÇÃO E PERSISTÊNCIA DA GOLD
# ==============================================================================
def step_gold_business():
    print(f"{'='*60}\n🏆 [STEP 04 -> 05] GOLD BUSINESS: tb_cliente\n{'='*60}")
    
    # 1.1 - LEITURA DA ENRICHED (O DADO JÁ LAPIDADO)
    origem = os.path.join(PATH_ENRICHED, "tb_cliente.parquet")
    
    if not os.path.exists(origem):
        print("❌ Erro: Camada Enriched não encontrada. O prédio ainda não tem alicerce.")
        return

    df = pd.read_parquet(origem)

    try:
        # 1.2 - GARANTIA DE ORDENAÇÃO (PRONTO PARA O CONSUMIDOR)
        # Embora o BI ordene, a Gold física deve ser previsível (ex: por ID)
        print("🔝 Ordenando registros para consumo final...")
        df = df.sort_values('id_cliente')

        # 1.3 - PERSISTÊNCIA FINAL (O PRODUTO ACABADO)
        # Salvamos como tb_cliente.parquet na pasta de negócio
        os.makedirs(PATH_GOLD, exist_ok=True)
        destino_gold = os.path.join(PATH_GOLD, "tb_cliente.parquet")
        
        # Aqui poderíamos salvar em outros formatos se o 'freguês' pedisse (CSV, SQL)
        # mas manteremos Parquet para performance do Power BI/Looker
        df.to_parquet(destino_gold, index=False, engine='pyarrow')

        # 1.4 - PROTOCOLO DE ENTREGA
        print(f"✅ Sucesso: Tabela 'tb_cliente' disponível na Gold com {len(df)} registros.")
        gerar_manifesto_gold(len(df), "SUCESSO")

    except Exception as e:
        print(f"❌ Erro na oficialização Gold: {e}")
        gerar_manifesto_gold(0, f"ERRO: {str(e)}")

if __name__ == "__main__":
    step_gold_business()