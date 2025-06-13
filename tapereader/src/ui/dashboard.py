"""
Dashboard do Tape Reader com visualização de books duplos
"""
import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
from collections import deque

logger = logging.getLogger(__name__)

class Dashboard:
    """Dashboard com visualização de books duplos (WDOFUT e DOLFUT)"""
    
    def __init__(self, engine: Any, config: Dict[str, Any]):
        self.engine = engine
        self.config = config
        self.is_running = False
        self.trade_history = deque(maxlen=10)
        self.wdo_book = {'bids': [], 'asks': []}
        self.dol_book = {'bids': [], 'asks': []}
        self.stats_history = deque(maxlen=60)
        
    async def initialize(self):
        """Inicializa o dashboard"""
        logger.info("Dashboard com book duplo inicializado")
        
    async def run(self):
        """Executa o dashboard com visualização de books separados"""
        self.is_running = True
        logger.info("Dashboard rodando com visualização de book duplo...")
        
        try:
            while self.is_running:
                # Limpa tela
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Cabeçalho
                self._print_header()
                
                # Obtém dados do provider para ter acesso aos books separados
                # Acessa diretamente o data_provider do engine
                if hasattr(self.engine, 'data_provider'):
                    try:
                        # Lê books separados
                        self.wdo_book = self.engine.data_provider._read_book('wdofut_book')
                        self.dol_book = self.engine.data_provider._read_book('dolfut_book')
                    except:
                        pass
                
                # Obtém dados do cache para trades
                market_data = await self.engine.cache_manager.get_market_data()
                
                if market_data:
                    # Atualiza histórico de trades
                    if market_data.get('trades'):
                        for trade in market_data['trades'][-5:]:
                            if trade not in self.trade_history:
                                self.trade_history.append(trade)
                
                # Mostra books lado a lado
                self._print_dual_books()
                
                # Mostra trades recentes
                self._print_recent_trades()
                
                # Mostra estatísticas
                self._print_stats()
                
                # Aguarda próxima atualização
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Dashboard interrompido pelo usuário")
        except Exception as e:
            logger.error(f"Erro no dashboard: {e}", exc_info=True)
        finally:
            self.is_running = False
    
    def _print_header(self):
        """Imprime cabeçalho"""
        print("=" * 120)
        print(f"{'TAPE READER PROFESSIONAL - BOOK DUPLO':^120}")
        print(f"{'Horário: ' + datetime.now().strftime('%H:%M:%S'):^120}")
        print("=" * 120)
        print()
    
    def _print_dual_books(self):
        """Mostra books de WDOFUT e DOLFUT lado a lado"""
        print("📊 BOOKS DE OFERTAS")
        print("=" * 120)
        
        # Cabeçalhos
        print(f"{'WDOFUT (MINI DÓLAR)':^58} │ {'DOLFUT (DÓLAR CHEIO)':^58}")
        print("-" * 58 + " │ " + "-" * 58)
        print(f"{'COMPRA':^28} │ {'VENDA':^28} │ {'COMPRA':^28} │ {'VENDA':^28}")
        print(f"{'Volume':>12} {'Preço':>14} │ {'Preço':>14} {'Volume':>12} │ "
              f"{'Volume':>12} {'Preço':>14} │ {'Preço':>14} {'Volume':>12}")
        print("-" * 58 + " │ " + "-" * 58)
        
        # Verifica se há dados
        if not any([self.wdo_book['bids'], self.wdo_book['asks'], 
                   self.dol_book['bids'], self.dol_book['asks']]):
            print(f"{'Aguardando dados...':^58} │ {'Aguardando dados...':^58}")
            print()
            return
        
        # Mostra 5 níveis
        max_levels = 5
        for i in range(max_levels):
            # WDOFUT
            wdo_bid = wdo_ask = " " * 28
            if i < len(self.wdo_book['bids']):
                bid = self.wdo_book['bids'][i]
                wdo_bid = f"{bid['volume']:>12} @ {bid['price']:>14.1f}"
            
            if i < len(self.wdo_book['asks']):
                ask = self.wdo_book['asks'][i]
                wdo_ask = f"{ask['price']:>14.1f} @ {ask['volume']:>12}"
            
            # DOLFUT
            dol_bid = dol_ask = " " * 28
            if i < len(self.dol_book['bids']):
                bid = self.dol_book['bids'][i]
                dol_bid = f"{bid['volume']:>12} @ {bid['price']:>14.2f}"
            
            if i < len(self.dol_book['asks']):
                ask = self.dol_book['asks'][i]
                dol_ask = f"{ask['price']:>14.2f} @ {ask['volume']:>12}"
            
            print(f"{wdo_bid} │ {wdo_ask} │ {dol_bid} │ {dol_ask}")
        
        # Linha separadora
        print("-" * 58 + " │ " + "-" * 58)
        
        # Spreads
        wdo_info = dol_info = "Sem dados"
        
        if self.wdo_book['bids'] and self.wdo_book['asks']:
            wdo_spread = self.wdo_book['asks'][0]['price'] - self.wdo_book['bids'][0]['price']
            wdo_mid = (self.wdo_book['asks'][0]['price'] + self.wdo_book['bids'][0]['price']) / 2
            wdo_info = f"Spread: {wdo_spread:.1f} pts │ Mid: {wdo_mid:.1f}"
        
        if self.dol_book['bids'] and self.dol_book['asks']:
            dol_spread = self.dol_book['asks'][0]['price'] - self.dol_book['bids'][0]['price']
            dol_mid = (self.dol_book['asks'][0]['price'] + self.dol_book['bids'][0]['price']) / 2
            dol_info = f"Spread: {dol_spread:.2f} │ Mid: {dol_mid:.2f}"
        
        print(f"{wdo_info:^58} │ {dol_info:^58}")
        
        # Análise de convergência
        if (self.wdo_book['bids'] and self.wdo_book['asks'] and 
            self.dol_book['bids'] and self.dol_book['asks']):
            wdo_mid = (self.wdo_book['asks'][0]['price'] + self.wdo_book['bids'][0]['price']) / 2
            dol_mid = (self.dol_book['asks'][0]['price'] + self.dol_book['bids'][0]['price']) / 2
            diff = abs(wdo_mid - dol_mid)
            
            if diff > 2.0:
                print(f"\n⚠️  DIVERGÊNCIA: {diff:.2f} pontos entre WDOFUT e DOLFUT")
            else:
                print(f"\n✓ Mercados convergentes (diferença: {diff:.2f} pontos)")
        
        print()
    
    def _print_recent_trades(self):
        """Mostra trades recentes"""
        print("📈 TRADES RECENTES")
        print("-" * 120)
        
        if not self.trade_history:
            print("   Aguardando trades...")
            print()
            return
        
        # Cabeçalho
        print(f"{'Hora':>12} {'Symbol':>8} {'Lado':>6} {'Preço':>10} {'Volume':>8}")
        print("-" * 60)
        
        # Mostra trades (mais recentes primeiro)
        for trade in reversed(self.trade_history):
            time_str = trade['timestamp'].split()[1] if ' ' in str(trade['timestamp']) else trade['timestamp']
            side_symbol = "▲" if trade['side'] == 'BUY' else "▼"
            
            print(f"{time_str:>12} {trade['symbol']:>8} {side_symbol:>6} "
                  f"{trade['price']:>10.1f} {trade['volume']:>8}")
        
        print()
    
    def _print_stats(self):
        """Mostra estatísticas"""
        stats = self.engine.get_stats()
        
        print("📊 ESTATÍSTICAS DO SISTEMA")
        print("-" * 120)
        
        # Layout em duas colunas
        col1 = []
        col2 = []
        
        # Coluna 1
        col1.append(f"Trades Processados: {stats.get('trades_processed', 0):,}")
        col1.append(f"Trades Salvos: {stats.get('trades_saved', 0):,}")
        col1.append(f"Duplicatas Prevenidas: {stats.get('trades_duplicates_prevented', 0):,}")
        
        # Coluna 2
        col2.append(f"Book Snapshots: {stats.get('book_snapshots_saved', 0):,}")
        if stats.get('trades_per_second', 0) > 0:
            col2.append(f"Trades/segundo: {stats.get('trades_per_second', 0):.2f}")
        else:
            col2.append("Trades/segundo: N/A")
        
        # Tempo de execução
        if stats.get('start_time'):
            runtime = datetime.now() - stats['start_time']
            hours, remainder = divmod(runtime.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            col2.append(f"Runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        
        # Imprime colunas
        max_lines = max(len(col1), len(col2))
        for i in range(max_lines):
            left = col1[i] if i < len(col1) else ""
            right = col2[i] if i < len(col2) else ""
            print(f"{left:<60}{right:<60}")
        
        # Status
        print("\nStatus: ", end="")
        if stats.get('trades_processed', 0) > 0:
            last_trade = stats.get('last_trade_time')
            if last_trade:
                time_since = (datetime.now() - last_trade).total_seconds()
                if time_since < 5:
                    print("🟢 ATIVO - Recebendo dados")
                elif time_since < 30:
                    print("🟡 LENTO - Poucos dados")
                else:
                    print("🔴 INATIVO - Sem dados novos")
            else:
                print("⚪ AGUARDANDO")
        else:
            print("⚪ INICIALIZANDO")
        
        print("=" * 120)
    
    async def close(self):
        """Fecha o dashboard"""
        self.is_running = False
        logger.info("Dashboard fechado")