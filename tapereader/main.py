#!/usr/bin/env python
"""
Tape Reader - Sistema de Leitura de Fluxo
"""
import sys
import os
import asyncio
import argparse
import logging
from datetime import datetime

# Adiciona o diretório pai ao path para permitir importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importações do projeto
from src.core.config import ConfigManager
from src.core.cache import CacheManager
from src.core.engine import TapeReadingEngine
from src.data.excel_provider import ExcelDataProvider
from src.ui.dashboard import Dashboard

# Configuração de logging
def setup_logging(level=logging.INFO):
    """Configura o sistema de logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Cria diretório de logs se não existir
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Handler para arquivo
    file_handler = logging.FileHandler(
        os.path.join(logs_dir, f'tape_reader_{datetime.now().strftime("%Y%m%d")}.log'),
        encoding='utf-8'  # Força UTF-8 no arquivo
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Handler para console com encoding UTF-8
    console_handler = logging.StreamHandler(sys.stdout)  # Usa stdout ao invés de stderr
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Tenta configurar o encoding do console para UTF-8 no Windows
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # Configura logger root
    logging.basicConfig(
        level=level,
        handlers=[file_handler, console_handler],
        encoding='utf-8'  # Define encoding padrão
    )
    
    # Ajusta níveis de loggers específicos
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('xlwings').setLevel(logging.WARNING)

async def main(args):
    """Função principal"""
    logger = logging.getLogger(__name__)
    
    try:
        # Inicializa gerenciadores
        logger.info(f"Iniciando Tape Reader em modo {args.mode}")
        
        # Carrega configurações
        # O config está dentro do diretório tapereader
        config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
        config_manager = ConfigManager(config_dir=config_dir, env=args.mode)
        
        # Inicializa cache
        cache_manager = CacheManager(config_manager.get('cache', {}))
        
        # Inicializa provider de dados
        excel_config = config_manager.get_provider_config('excel')
        if not excel_config:
            logger.error("Configuração do Excel não encontrada!")
            return
            
        logger.info(f"Configuração Excel carregada: {excel_config}")
        data_provider = ExcelDataProvider(excel_config, cache_manager)
        await data_provider.initialize()
        
        # Inicializa engine
        engine = TapeReadingEngine(
            config=config_manager.get_all(),
            data_provider=data_provider,
            cache_manager=cache_manager
        )
        
        # Inicializa dashboard se não for modo headless
        dashboard = None
        if not args.headless:
            dashboard = Dashboard(engine, config_manager.get('ui', {}))
            await dashboard.initialize()
        
        # Inicia engine
        logger.info("Iniciando engine de tape reading...")
        
        # Cria tasks assíncronas
        tasks = [engine.start()]
        if dashboard:
            tasks.append(dashboard.run())
        
        # Executa tasks
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
    finally:
        # Cleanup
        logger.info("Encerrando sistema...")
        if 'engine' in locals():
            await engine.stop()
        if 'data_provider' in locals():
            await data_provider.close()
        if 'dashboard' in locals() and dashboard:
            await dashboard.close()

def parse_arguments():
    """Parse argumentos da linha de comando"""
    parser = argparse.ArgumentParser(description='Tape Reader - Sistema de Leitura de Fluxo')
    
    parser.add_argument(
        '--mode',
        choices=['development', 'production'],
        default='production',
        help='Modo de execução (default: production)'
    )
    
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Executa sem interface gráfica'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Ativa modo debug com logging detalhado'
    )
    
    return parser.parse_args()

if __name__ == '__main__':
    # Parse argumentos
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(log_level)
    
    # Executa aplicação
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\nEncerrando aplicação...")
    except Exception as e:
        print(f"Erro ao executar aplicação: {e}")
        sys.exit(1)