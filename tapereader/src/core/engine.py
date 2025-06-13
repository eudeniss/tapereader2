"""
Engine principal do Tape Reader com persistência em banco de dados e proteção contra duplicatas
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Set
from ..data.database import DatabaseManager

logger = logging.getLogger(__name__)

class TapeReadingEngine:
    """Engine principal de leitura de fluxo com persistência"""
    
    def __init__(self, config: Dict[str, Any], data_provider: Any, cache_manager: Any):
        self.config = config
        self.data_provider = data_provider
        self.cache_manager = cache_manager
        self.is_running = False
        self.stats = {
            'trades_processed': 0,
            'trades_saved': 0,
            'trades_duplicates_prevented': 0,
            'book_snapshots_saved': 0,
            'start_time': None,
            'last_trade_time': None,
            'validation_errors': 0
        }
        
        # Inicializa gerenciador de banco de dados
        db_path = config.get('database', {}).get('path', 'data/tape_reader.db')
        self.db_manager = DatabaseManager(db_path)
        
        # Configurações de persistência
        self.save_interval = config.get('database', {}).get('save_interval', 10)
        self.save_book_snapshots = config.get('database', {}).get('save_book_snapshots', True)
        self.validate_trades = config.get('database', {}).get('validate_trades', True)
        
        # Buffer de trades para salvar em batch
        self.trade_buffer = []
        self.last_book_snapshot = {}
        
        # Controle adicional de duplicatas (última linha de defesa)
        self.saved_trade_hashes: Set[str] = set()
        self.max_saved_hashes = 20000
        
    async def start(self):
        """Inicia a engine"""
        logger.info("Iniciando engine de tape reading...")
        
        # Inicializa banco de dados
        await self.db_manager.initialize()
        
        # Carrega hashes dos últimos trades salvos para evitar duplicatas
        await self._load_recent_trade_hashes()
        
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        
        try:
            while self.is_running:
                # Obtém dados do provider
                data = await self.data_provider.get_data()
                
                if data:
                    # Processa trades
                    if data.get('trades'):
                        await self._process_trades(data['trades'])
                    
                    # Processa book
                    if data.get('book') and self.save_book_snapshots:
                        await self._process_book(data['book'])
                    
                    # Atualiza cache
                    await self.cache_manager.update_market_data(data)
                    
                    # Atualiza estatísticas
                    self.stats['last_trade_time'] = datetime.now()
                
                # Aguarda próximo ciclo
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Erro na engine: {e}", exc_info=True)
        finally:
            # Salva trades pendentes antes de fechar
            if self.trade_buffer:
                await self._save_trades_batch()
                
            self.is_running = False
    
    async def _load_recent_trade_hashes(self):
        """Carrega hashes dos trades recentes do banco para evitar duplicatas"""
        try:
            cursor = self.db_manager.conn.cursor()
            
            # Carrega hashes dos últimos 1000 trades
            cursor.execute("""
                SELECT timestamp, symbol, side, price, volume 
                FROM trades 
                ORDER BY id DESC 
                LIMIT 1000
            """)
            
            for row in cursor.fetchall():
                trade_hash = self._generate_trade_hash({
                    'timestamp': row['timestamp'],
                    'symbol': row['symbol'],
                    'side': row['side'],
                    'price': row['price'],
                    'volume': row['volume']
                })
                self.saved_trade_hashes.add(trade_hash)
            
            logger.info(f"Carregados {len(self.saved_trade_hashes)} hashes de trades recentes")
            
        except Exception as e:
            logger.error(f"Erro ao carregar hashes recentes: {e}")
    
    def _generate_trade_hash(self, trade: Dict) -> str:
        """Gera hash único para um trade"""
        import hashlib
        key_parts = [
            str(trade.get('timestamp', '')),
            str(trade.get('symbol', '')),
            str(trade.get('side', '')),
            str(trade.get('price', '')),
            str(trade.get('volume', ''))
        ]
        trade_str = '_'.join(key_parts)
        return hashlib.md5(trade_str.encode()).hexdigest()
    
    async def stop(self):
        """Para a engine"""
        logger.info("Parando engine...")
        self.is_running = False
        
        # Salva dados pendentes
        if self.trade_buffer:
            await self._save_trades_batch()
            
        # Fecha banco de dados
        await self.db_manager.close()
        
    async def _process_trades(self, trades: list):
        """Processa lista de trades com verificação de duplicatas"""
        duplicates_prevented = 0
        
        for trade in trades:
            # Gera hash do trade
            trade_hash = self._generate_trade_hash(trade)
            
            # Verifica se já foi salvo (última linha de defesa)
            if trade_hash in self.saved_trade_hashes:
                duplicates_prevented += 1
                logger.debug(f"Duplicata prevenida: {trade}")
                continue
            
            # Trade é único, processa
            self.stats['trades_processed'] += 1
            
            # Adiciona ao buffer
            self.trade_buffer.append(trade)
            
            # Adiciona hash ao conjunto
            self.saved_trade_hashes.add(trade_hash)
            
            # Atualiza níveis de preço
            await self.db_manager.update_price_levels(
                trade['symbol'], 
                trade['price'], 
                trade['volume']
            )
            
            # Log de debug a cada 10 trades
            if self.stats['trades_processed'] % 10 == 0:
                logger.debug(f"Trades processados: {self.stats['trades_processed']}")
        
        if duplicates_prevented > 0:
            self.stats['trades_duplicates_prevented'] += duplicates_prevented
            logger.warning(f"Prevenidas {duplicates_prevented} duplicatas neste batch")
        
        # Limpa hashes antigos se necessário
        if len(self.saved_trade_hashes) > self.max_saved_hashes:
            # Mantém apenas metade dos hashes
            self.saved_trade_hashes = set(list(self.saved_trade_hashes)[-(self.max_saved_hashes // 2):])
            logger.info("Limpeza de hashes antigos realizada")
        
        # Salva em batch quando atinge o intervalo
        if len(self.trade_buffer) >= self.save_interval:
            await self._save_trades_batch()
            
    async def _save_trades_batch(self):
        """Salva batch de trades no banco"""
        if not self.trade_buffer:
            return
            
        try:
            # Salva trades
            saved_count = await self.db_manager.save_trades(self.trade_buffer)
            self.stats['trades_saved'] += saved_count
            
            # Atualiza estatísticas de mercado
            await self.db_manager.update_market_stats(self.trade_buffer)
            
            # Validação (se habilitada)
            if self.validate_trades and saved_count != len(self.trade_buffer):
                logger.warning(f"Validação: Esperados {len(self.trade_buffer)} trades, salvos {saved_count}")
                self.stats['validation_errors'] += 1
            
            logger.info(f"Salvos {saved_count} trades no banco de dados")
            
            # Limpa buffer
            self.trade_buffer.clear()
            
        except Exception as e:
            logger.error(f"Erro ao salvar trades: {e}")
            
    async def _process_book(self, book: Dict[str, Any]):
        """Processa snapshot do book"""
        try:
            # Detecta mudanças significativas no book
            if self._book_changed_significantly(book):
                # Salva snapshot para cada símbolo ativo
                for symbol in ['WDOFUT', 'DOLFUT']:
                    if book.get('bids') and book.get('asks'):
                        success = await self.db_manager.save_book_snapshot(symbol, book)
                        if success:
                            self.stats['book_snapshots_saved'] += 1
                            
                # Atualiza último snapshot
                self.last_book_snapshot = book
                
        except Exception as e:
            logger.error(f"Erro ao processar book: {e}")
            
    def _book_changed_significantly(self, current_book: Dict) -> bool:
        """Verifica se o book mudou significativamente"""
        if not self.last_book_snapshot:
            return True
            
        # Compara melhor bid/ask
        try:
            last_bids = self.last_book_snapshot.get('bids', [])
            last_asks = self.last_book_snapshot.get('asks', [])
            curr_bids = current_book.get('bids', [])
            curr_asks = current_book.get('asks', [])
            
            if not (last_bids and last_asks and curr_bids and curr_asks):
                return True
                
            # Mudança de preço no topo do book
            if (last_bids[0]['price'] != curr_bids[0]['price'] or
                last_asks[0]['price'] != curr_asks[0]['price']):
                return True
                
            # Mudança significativa de volume (mais de 50%)
            bid_vol_change = abs(last_bids[0]['volume'] - curr_bids[0]['volume']) / last_bids[0]['volume']
            ask_vol_change = abs(last_asks[0]['volume'] - curr_asks[0]['volume']) / last_asks[0]['volume']
            
            if bid_vol_change > 0.5 or ask_vol_change > 0.5:
                return True
                
        except:
            return True
            
        return False
        
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas da engine"""
        stats = self.stats.copy()
        
        # Adiciona estatísticas calculadas
        if stats['start_time']:
            runtime = (datetime.now() - stats['start_time']).total_seconds()
            stats['trades_per_second'] = stats['trades_processed'] / runtime if runtime > 0 else 0
            stats['save_rate'] = (stats['trades_saved'] / stats['trades_processed'] * 100) if stats['trades_processed'] > 0 else 0
            stats['duplicate_prevention_rate'] = (stats['trades_duplicates_prevented'] / (stats['trades_processed'] + stats['trades_duplicates_prevented']) * 100) if (stats['trades_processed'] + stats['trades_duplicates_prevented']) > 0 else 0
            
        return stats
        
    async def get_support_resistance(self, symbol: str) -> Dict[str, list]:
        """Retorna níveis de suporte e resistência identificados"""
        return await self.db_manager.get_support_resistance_levels(symbol)
        
    async def validate_data_integrity(self) -> Dict[str, Any]:
        """Executa validação de integridade dos dados"""
        validation_results = {
            'timestamp': datetime.now(),
            'trades_in_buffer': len(self.trade_buffer),
            'stats': self.get_stats(),
            'database_connected': self.db_manager.conn is not None,
            'saved_hashes_count': len(self.saved_trade_hashes),
            'duplicates_prevented_total': self.stats['trades_duplicates_prevented']
        }
        
        return validation_results