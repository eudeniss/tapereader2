#!/usr/bin/env python
"""
Monitor de Trades em Tempo Real - Versão Limpa com Books Duplos
Visualização separada de WDOFUT e DOLFUT sem colunas extras
"""
import asyncio
import sys
import os
from datetime import datetime
from collections import deque

# Adiciona o diretório ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.config import ConfigManager
from src.core.cache import CacheManager
from src.data.excel_provider import ExcelDataProvider
from src.analysis.analyzers import VolumeAnalyzer, PriceActionAnalyzer, OrderFlowAnalyzer, ImbalanceAnalyzer

# Cores para terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

async def monitor_trades():
    """Monitora trades em tempo real com visualização limpa"""
    print(f"{Colors.BOLD}=== MONITOR DE TRADES - WDOFUT & DOLFUT ==={Colors.RESET}\n")
    
    try:
        # Carrega configurações
        config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
        config_manager = ConfigManager(config_dir=config_dir, env='production')
        
        # Inicializa cache e provider
        cache_manager = CacheManager(config_manager.get('cache', {}))
        await cache_manager.initialize()
        
        excel_config = config_manager.get_provider_config('excel')
        data_provider = ExcelDataProvider(excel_config, cache_manager)
        await data_provider.initialize()
        
        # Inicializa analisadores
        volume_analyzer = VolumeAnalyzer({})
        price_analyzer = PriceActionAnalyzer({})
        flow_analyzer = OrderFlowAnalyzer({})
        imbalance_analyzer = ImbalanceAnalyzer({})
        
        print(f"{Colors.GREEN}✅ Sistema inicializado{Colors.RESET}\n")
        print("Monitorando trades... (Ctrl+C para parar)\n")
        print("-" * 120)
        
        # Histórico separado por símbolo
        wdo_trades = deque(maxlen=15)
        dol_trades = deque(maxlen=15)
        
        # Estatísticas
        stats = {
            'WDOFUT': {'total': 0, 'buy': 0, 'sell': 0},
            'DOLFUT': {'total': 0, 'buy': 0, 'sell': 0}
        }
        
        while True:
            # Lê dados brutos para ter acesso aos books separados
            all_data = {
                'timestamp': datetime.now(),
                'wdofut': {
                    'trades': data_provider._read_trades('wdofut_trades'),
                    'book': data_provider._read_book('wdofut_book')
                },
                'dolfut': {
                    'trades': data_provider._read_trades('dolfut_trades'),
                    'book': data_provider._read_book('dolfut_book')
                }
            }
            
            # Também pega dados processados para novos trades
            processed_data = await data_provider.get_data()
            
            # Limpa tela
            os.system('cls' if os.name == 'nt' else 'clear')
            
            print(f"{Colors.BOLD}=== MONITOR DE TRADES - WDOFUT & DOLFUT ==={Colors.RESET}")
            print(f"Horário: {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 120)
            
            # Processa novos trades
            if processed_data and processed_data.get('trades'):
                new_trades = processed_data['trades']
                
                # Separa por símbolo
                for trade in new_trades:
                    if trade['symbol'] == 'WDOFUT':
                        wdo_trades.append(trade)
                        stats['WDOFUT']['total'] += 1
                        if trade['side'] == 'BUY':
                            stats['WDOFUT']['buy'] += trade['volume']
                        else:
                            stats['WDOFUT']['sell'] += trade['volume']
                    else:
                        dol_trades.append(trade)
                        stats['DOLFUT']['total'] += 1
                        if trade['side'] == 'BUY':
                            stats['DOLFUT']['buy'] += trade['volume']
                        else:
                            stats['DOLFUT']['sell'] += trade['volume']
                
                # Análises
                volume_analysis = await volume_analyzer.analyze(new_trades)
                price_analysis = await price_analyzer.analyze(new_trades)
                flow_analysis = await flow_analyzer.analyze(new_trades)
            
            # SEÇÃO 1: TRADES LADO A LADO
            print(f"\n{Colors.BOLD}📈 TRADES RECENTES{Colors.RESET}")
            print("-" * 120)
            print(f"{Colors.CYAN}{'WDOFUT':^58}{Colors.RESET} │ {Colors.MAGENTA}{'DOLFUT':^58}{Colors.RESET}")
            print("-" * 58 + " │ " + "-" * 58)
            print(f"{'Hora':>12} {'Lado':>6} {'Preço':>12} {'Volume':>8} │ "
                  f"{'Hora':>12} {'Lado':>6} {'Preço':>12} {'Volume':>8}")
            print("-" * 58 + " │ " + "-" * 58)
            
            # Mostra trades
            max_trades = max(len(wdo_trades), len(dol_trades))
            for i in range(min(max_trades, 10)):  # Mostra até 10 trades
                # WDOFUT
                wdo_line = " " * 58
                if i < len(wdo_trades):
                    trade = list(reversed(wdo_trades))[i]
                    time_str = trade['timestamp'].split()[1] if ' ' in str(trade['timestamp']) else trade['timestamp']
                    side_color = Colors.GREEN if trade['side'] == 'BUY' else Colors.RED
                    side_symbol = '▲' if trade['side'] == 'BUY' else '▼'
                    
                    wdo_line = (f"{time_str:>12} {side_color}{side_symbol:>6}{Colors.RESET} "
                               f"{trade['price']:>12.1f} {trade['volume']:>8}")
                
                # DOLFUT
                dol_line = " " * 58
                if i < len(dol_trades):
                    trade = list(reversed(dol_trades))[i]
                    time_str = trade['timestamp'].split()[1] if ' ' in str(trade['timestamp']) else trade['timestamp']
                    side_color = Colors.GREEN if trade['side'] == 'BUY' else Colors.RED
                    side_symbol = '▲' if trade['side'] == 'BUY' else '▼'
                    
                    dol_line = (f"{time_str:>12} {side_color}{side_symbol:>6}{Colors.RESET} "
                               f"{trade['price']:>12.2f} {trade['volume']:>8}")
                
                print(f"{wdo_line} │ {dol_line}")
            
            # SEÇÃO 2: BOOKS LADO A LADO
            print(f"\n{Colors.BOLD}📊 BOOKS DE OFERTAS{Colors.RESET}")
            print("-" * 120)
            
            wdo_book = all_data['wdofut']['book']
            dol_book = all_data['dolfut']['book']
            
            # Análise de desequilíbrio
            wdo_imbalance = await imbalance_analyzer.analyze(wdo_book, list(wdo_trades))
            dol_imbalance = await imbalance_analyzer.analyze(dol_book, list(dol_trades))
            
            # Cabeçalhos
            print(f"{Colors.CYAN}{'WDOFUT':^58}{Colors.RESET} │ {Colors.MAGENTA}{'DOLFUT':^58}{Colors.RESET}")
            print(f"{'COMPRA':^28} │ {'VENDA':^28} │ {'COMPRA':^28} │ {'VENDA':^28}")
            print(f"{'Volume':>12} {'Preço':>14} │ {'Preço':>14} {'Volume':>12} │ "
                  f"{'Volume':>12} {'Preço':>14} │ {'Preço':>14} {'Volume':>12}")
            print("-" * 58 + " │ " + "-" * 58)
            
            # Mostra 5 níveis
            for i in range(5):
                # WDOFUT
                wdo_bid_str = wdo_ask_str = " " * 28
                if i < len(wdo_book['bids']):
                    bid = wdo_book['bids'][i]
                    wdo_bid_str = f"{bid['volume']:>12} @ {Colors.GREEN}{bid['price']:>12.1f}{Colors.RESET}"
                
                if i < len(wdo_book['asks']):
                    ask = wdo_book['asks'][i]
                    wdo_ask_str = f"{Colors.RED}{ask['price']:>12.1f}{Colors.RESET} @ {ask['volume']:>12}"
                
                # DOLFUT
                dol_bid_str = dol_ask_str = " " * 28
                if i < len(dol_book['bids']):
                    bid = dol_book['bids'][i]
                    dol_bid_str = f"{bid['volume']:>12} @ {Colors.GREEN}{bid['price']:>12.2f}{Colors.RESET}"
                
                if i < len(dol_book['asks']):
                    ask = dol_book['asks'][i]
                    dol_ask_str = f"{Colors.RED}{ask['price']:>12.2f}{Colors.RESET} @ {ask['volume']:>12}"
                
                print(f"{wdo_bid_str} │ {wdo_ask_str} │ {dol_bid_str} │ {dol_ask_str}")
            
            # Spreads e imbalances
            print("-" * 58 + " │ " + "-" * 58)
            
            # WDOFUT info
            wdo_info = "Sem dados"
            if wdo_book['bids'] and wdo_book['asks']:
                wdo_spread = wdo_book['asks'][0]['price'] - wdo_book['bids'][0]['price']
                wdo_mid = (wdo_book['asks'][0]['price'] + wdo_book['bids'][0]['price']) / 2
                
                imb = wdo_imbalance.get('book_imbalance', {})
                if imb.get('is_imbalanced'):
                    imb_color = Colors.YELLOW
                    imb_text = f" {imb_color}[{imb.get('direction', '')}]{Colors.RESET}"
                else:
                    imb_text = ""
                
                wdo_info = f"Spread: {wdo_spread:.1f} | Mid: {wdo_mid:.1f}{imb_text}"
            
            # DOLFUT info
            dol_info = "Sem dados"
            if dol_book['bids'] and dol_book['asks']:
                dol_spread = dol_book['asks'][0]['price'] - dol_book['bids'][0]['price']
                dol_mid = (dol_book['asks'][0]['price'] + dol_book['bids'][0]['price']) / 2
                
                imb = dol_imbalance.get('book_imbalance', {})
                if imb.get('is_imbalanced'):
                    imb_color = Colors.YELLOW
                    imb_text = f" {imb_color}[{imb.get('direction', '')}]{Colors.RESET}"
                else:
                    imb_text = ""
                
                dol_info = f"Spread: {dol_spread:.2f} | Mid: {dol_mid:.2f}{imb_text}"
            
            print(f"{wdo_info:^68} │ {dol_info:^68}")
            
            # SEÇÃO 3: ESTATÍSTICAS E ANÁLISES
            print(f"\n{Colors.BOLD}📊 ESTATÍSTICAS{Colors.RESET}")
            print("-" * 120)
            
            # Volume delta por símbolo
            wdo_delta = stats['WDOFUT']['buy'] - stats['WDOFUT']['sell']
            dol_delta = stats['DOLFUT']['buy'] - stats['DOLFUT']['sell']
            
            print(f"{Colors.CYAN}WDOFUT:{Colors.RESET} Trades: {stats['WDOFUT']['total']:>5} | "
                  f"Buy: {stats['WDOFUT']['buy']:>6} | Sell: {stats['WDOFUT']['sell']:>6} | "
                  f"Delta: {wdo_delta:>+7}")
            
            print(f"{Colors.MAGENTA}DOLFUT:{Colors.RESET} Trades: {stats['DOLFUT']['total']:>5} | "
                  f"Buy: {stats['DOLFUT']['buy']:>6} | Sell: {stats['DOLFUT']['sell']:>6} | "
                  f"Delta: {dol_delta:>+7}")
            
            # Análises dos analisadores
            if flow_analysis:
                bias_color = Colors.GREEN if flow_analysis.get('flow_bias') == 'BULLISH' else Colors.RED if flow_analysis.get('flow_bias') == 'BEARISH' else Colors.YELLOW
                print(f"\nFluxo Geral: {bias_color}{flow_analysis.get('flow_bias', 'NEUTRAL')}{Colors.RESET} | "
                      f"Delta: {flow_analysis.get('volume_delta', 0):+} ({flow_analysis.get('delta_percent', 0):+.1f}%)")
            
            if price_analysis:
                trend_color = Colors.GREEN if price_analysis.get('price_trend') == 'BULLISH' else Colors.RED if price_analysis.get('price_trend') == 'BEARISH' else Colors.YELLOW
                print(f"Tendência: {trend_color}{price_analysis.get('price_trend', 'NEUTRAL')}{Colors.RESET} | "
                      f"Volatilidade: {price_analysis.get('volatility', 0):.2f}%")
            
            if volume_analysis:
                vol_trend = volume_analysis.get('volume_trend', 'NEUTRAL')
                vol_color = Colors.GREEN if vol_trend == 'INCREASING' else Colors.RED if vol_trend == 'DECREASING' else Colors.YELLOW
                print(f"Volume: {vol_color}{vol_trend}{Colors.RESET}")
            
            # Convergência/Divergência
            if (wdo_book['bids'] and wdo_book['asks'] and 
                dol_book['bids'] and dol_book['asks']):
                wdo_mid = (wdo_book['asks'][0]['price'] + wdo_book['bids'][0]['price']) / 2
                dol_mid = (dol_book['asks'][0]['price'] + dol_book['bids'][0]['price']) / 2
                diff = abs(wdo_mid - dol_mid)
                
                if diff > 2.0:
                    print(f"\n{Colors.YELLOW}⚠️  DIVERGÊNCIA: {diff:.2f} pontos{Colors.RESET}")
                else:
                    print(f"\n{Colors.GREEN}✓ Mercados convergentes (diff: {diff:.2f}){Colors.RESET}")
            
            print("=" * 120)
            
            # Aguarda próxima leitura
            await asyncio.sleep(0.5)
            
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Monitor interrompido pelo usuário{Colors.RESET}")
    except Exception as e:
        print(f"\n{Colors.RED}❌ Erro: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
    finally:
        if 'data_provider' in locals():
            await data_provider.close()
        print("\nMonitor encerrado.")

if __name__ == "__main__":
    # Executa o monitor
    asyncio.run(monitor_trades())