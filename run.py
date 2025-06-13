#!/usr/bin/env python
"""
Script para executar o sistema completo de Tape Reading
Roda o processamento principal e a detecção de padrões
"""
import asyncio
import sys
import os
import subprocess
from datetime import datetime

# Cores para terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header():
    """Imprime cabeçalho do sistema"""
    print(f"{Colors.BOLD}")
    print("=" * 80)
    print("        TAPE READER PROFESSIONAL - SISTEMA INTEGRADO")
    print("        Absorção & Exaustão | Análise de Fluxo em Tempo Real")
    print("=" * 80)
    print(f"{Colors.RESET}")

def print_instructions():
    """Imprime instruções de uso"""
    print(f"{Colors.BOLD}INSTRUÇÕES:{Colors.RESET}")
    print()
    print("1. O sistema abrirá 3 janelas de terminal:")
    print(f"   {Colors.BLUE}• Terminal 1{Colors.RESET}: Sistema principal (processa e salva dados)")
    print(f"   {Colors.GREEN}• Terminal 2{Colors.RESET}: Tape Reading (detecta padrões)")
    print(f"   {Colors.YELLOW}• Terminal 3{Colors.RESET}: Monitor visual (opcional)")
    print()
    print("2. Certifique-se de que o Excel está aberto com RTD ativo")
    print()
    print("3. Para parar, feche as janelas ou pressione Ctrl+C")
    print()
    print("-" * 80)

def main():
    """Função principal"""
    print_header()
    print_instructions()
    
    # Diretório base
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        # Pergunta ao usuário
        print(f"{Colors.BOLD}Escolha o modo de execução:{Colors.RESET}")
        print("1. Sistema Completo (3 terminais)")
        print("2. Apenas Tape Reading (2 terminais)")
        print("3. Apenas Processamento (1 terminal)")
        
        choice = input("\nOpção (1-3): ").strip()
        
        if choice == "1":
            # Sistema completo
            print(f"\n{Colors.GREEN}Iniciando sistema completo...{Colors.RESET}")
            
            # Terminal 1: Sistema principal
            print(f"{Colors.BLUE}▶ Iniciando sistema principal...{Colors.RESET}")
            cmd1 = f'start cmd /k "cd /d {base_dir} && python tapereader/main.py --mode production"'
            subprocess.Popen(cmd1, shell=True)
            
            # Aguarda um pouco
            print("Aguardando inicialização...")
            asyncio.run(asyncio.sleep(3))
            
            # Terminal 2: Tape Reading
            print(f"{Colors.GREEN}▶ Iniciando tape reading...{Colors.RESET}")
            cmd2 = f'start cmd /k "cd /d {base_dir} && python tapereader/tape_reading_live.py"'
            subprocess.Popen(cmd2, shell=True)
            
            # Terminal 3: Monitor (opcional)
            monitor = input("\nAbrir monitor visual? (s/n): ").strip().lower()
            if monitor == 's':
                print(f"{Colors.YELLOW}▶ Iniciando monitor...{Colors.RESET}")
                cmd3 = f'start cmd /k "cd /d {base_dir} && python tapereader/monitor_clean.py"'
                subprocess.Popen(cmd3, shell=True)
            
        elif choice == "2":
            # Apenas tape reading
            print(f"\n{Colors.GREEN}Iniciando tape reading...{Colors.RESET}")
            
            # Terminal 1: Sistema principal
            print(f"{Colors.BLUE}▶ Iniciando sistema principal...{Colors.RESET}")
            cmd1 = f'start cmd /k "cd /d {base_dir} && python tapereader/main.py --mode production --headless"'
            subprocess.Popen(cmd1, shell=True)
            
            asyncio.run(asyncio.sleep(3))
            
            # Terminal 2: Tape Reading
            print(f"{Colors.GREEN}▶ Iniciando detecção de padrões...{Colors.RESET}")
            cmd2 = f'start cmd /k "cd /d {base_dir} && python tapereader/tape_reading_live.py"'
            subprocess.Popen(cmd2, shell=True)
            
        elif choice == "3":
            # Apenas processamento
            print(f"\n{Colors.BLUE}Iniciando processamento...{Colors.RESET}")
            cmd = f'cd /d {base_dir} && python tapereader/main.py --mode production'
            subprocess.run(cmd, shell=True)
            
        else:
            print(f"{Colors.RED}Opção inválida!{Colors.RESET}")
            return
            
        print(f"\n{Colors.GREEN}✅ Sistema iniciado com sucesso!{Colors.RESET}")
        print("\nOs terminais estão rodando em segundo plano.")
        print("Para parar, feche as janelas dos terminais.")
        
        # Mantém o script rodando
        input("\nPressione Enter para sair deste menu...")
        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Sistema interrompido pelo usuário{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}Erro: {e}{Colors.RESET}")

if __name__ == "__main__":
    # Se não for Windows, ajusta comandos
    if sys.platform != "win32":
        print("Este script foi otimizado para Windows.")
        print("Em Linux/Mac, execute os comandos manualmente:")
        print("1. python tapereader/main.py --mode production")
        print("2. python tapereader/tape_reading_live.py")
        print("3. python tapereader/monitor_clean.py")
    else:
        main()