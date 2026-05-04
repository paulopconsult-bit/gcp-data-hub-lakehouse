# ==============================================================================
# BIBLIOTECAS (O Comando Central)
# ==============================================================================
import subprocess # Para disparar os scripts como processos independentes
import os         # Para mapeamento de caminhos e variáveis de ambiente
import sys        # Para garantir o uso do interpretador Python correto

# ==============================================================================
# STEP 0: ABSTRAÇÃO DE AMBIENTE E MAPEAMENTO DE SCRIPTS
# ==============================================================================
# O Orquestrador precisa saber onde ele mesmo está para achar os "irmãos"
# Usamos o caminho absoluto do diretório onde este arquivo reside
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Lista da Esteira (Pipeline) na ordem exata de dependência
# O nome dos arquivos deve bater 100% com o que está na sua pasta
PIPELINE_CLIENTE = [
    "01-raw-02-bronze-cliente.py",
    "02-bronze-03-silver-cleaned-cliente.py",
    "03-silver-clean-04-silver-enriched-cliente.py",
    "04-silver-enriched-05-gold-cliente.py"
]

# ==============================================================================
# STEP 1: EXECUÇÃO DA ESTEIRA (FLOW CONTROL)
# ==============================================================================
def rodar_pipeline():
    print(f"{'='*60}")
    print(f"🎼 MAESTRO: ORQUESTRAÇÃO DE DOMÍNIO [00-CLIENTE]")
    print(f"📍 LOCALIZAÇÃO: {SCRIPT_DIR}")
    print(f"{'='*60}\n")

    for script in PIPELINE_CLIENTE:
        # Construímos o caminho absoluto para evitar erros de "arquivo não encontrado"
        caminho_script = os.path.join(SCRIPT_DIR, script)
        
        print(f"➔ 🚀 INICIANDO: {script}")
        
        # Executamos o script. 
        # check=False nos permite capturar o erro e parar a esteira manualmente
        processo = subprocess.run([sys.executable, caminho_script], capture_output=False)

        # Lógica de Interrupção (Fail-Fast)
        if processo.returncode == 0:
            print(f"✅ SUCESSO: {script} concluído.\n")
        else:
            print(f"\n{'!'*60}")
            print(f"❌ ERRO CRÍTICO NO STEP: {script}")
            print(f"🚫 A esteira foi interrompida para evitar corrupção de dados.")
            print(f"{'!'*60}")
            sys.exit(1) # Finaliza o orquestrador com código de erro

    print(f"{'='*60}")
    print(f"🏆 VITÓRIA: Toda a esteira Cliente foi processada com sucesso!")
    print(f"{'='*60}")

if __name__ == "__main__":
    rodar_pipeline()