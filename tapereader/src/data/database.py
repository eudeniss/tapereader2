"""
Sistema de persistência de dados com SQLite
"""
import sqlite3
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gerenciador do banco de dados SQLite"""
    
    def __init__(self, db_path: str = "data/tape_reader.db"):
        self.db_path = db_path
        self.conn = None
        self._ensure_directory()
        
    def _ensure_directory(self):
        """Garante que o diretório existe"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
    async def initialize(self):
        """Inicializa conexão e cria tabelas"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row  # Retorna linhas como dicionários
            
            # Habilita WAL mode para melhor performance
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            
            await self._create_tables()
            logger.info(f"Banco de dados inicializado: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Erro ao inicializar banco: {e}")
            raise
            
    async def _create_tables(self):
        """Cria estrutura das tabelas"""
        
        # Tabela de trades
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                volume INTEGER NOT NULL,
                aggressor BOOLEAN NOT NULL,
                order_id TEXT,
                row_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Índices para performance
                CHECK (side IN ('BUY', 'SELL')),
                CHECK (symbol IN ('WDOFUT', 'DOLFUT'))
            )
        """)
        
        # Tabela de snapshots do book
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS book_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                bid_price_1 REAL,
                bid_volume_1 INTEGER,
                bid_price_2 REAL,
                bid_volume_2 INTEGER,
                bid_price_3 REAL,
                bid_volume_3 INTEGER,
                bid_price_4 REAL,
                bid_volume_4 INTEGER,
                bid_price_5 REAL,
                bid_volume_5 INTEGER,
                ask_price_1 REAL,
                ask_volume_1 INTEGER,
                ask_price_2 REAL,
                ask_volume_2 INTEGER,
                ask_price_3 REAL,
                ask_volume_3 INTEGER,
                ask_price_4 REAL,
                ask_volume_4 INTEGER,
                ask_price_5 REAL,
                ask_volume_5 INTEGER,
                spread REAL,
                mid_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabela de níveis de preço (para suporte/resistência)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS price_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                level_type TEXT NOT NULL,
                strength INTEGER DEFAULT 1,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                touch_count INTEGER DEFAULT 1,
                volume_traded INTEGER DEFAULT 0,
                
                CHECK (level_type IN ('SUPPORT', 'RESISTANCE', 'PIVOT')),
                UNIQUE(symbol, price, level_type)
            )
        """)
        
        # Tabela de validação (para batimento)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                check_type TEXT NOT NULL,
                expected_count INTEGER,
                actual_count INTEGER,
                missing_trades TEXT,
                extra_trades TEXT,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CHECK (status IN ('OK', 'WARNING', 'ERROR'))
            )
        """)
        
        # Tabela de estatísticas agregadas
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS market_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                hour INTEGER NOT NULL,
                total_trades INTEGER DEFAULT 0,
                total_volume INTEGER DEFAULT 0,
                buy_trades INTEGER DEFAULT 0,
                sell_trades INTEGER DEFAULT 0,
                buy_volume INTEGER DEFAULT 0,
                sell_volume INTEGER DEFAULT 0,
                high_price REAL,
                low_price REAL,
                open_price REAL,
                close_price REAL,
                vwap REAL,
                
                UNIQUE(symbol, date, hour)
            )
        """)
        
        # Criar índices para performance
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp ON trades(symbol, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_book_timestamp ON book_snapshots(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_book_symbol ON book_snapshots(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_levels_symbol_price ON price_levels(symbol, price)",
            "CREATE INDEX IF NOT EXISTS idx_stats_symbol_date ON market_stats(symbol, date)"
        ]
        
        for idx in indices:
            self.conn.execute(idx)
            
        self.conn.commit()
        
    async def save_trades(self, trades: List[Dict[str, Any]]) -> int:
        """Salva lista de trades no banco"""
        if not trades:
            return 0
            
        try:
            cursor = self.conn.cursor()
            
            # Prepara dados para inserção
            trade_data = []
            for trade in trades:
                trade_data.append((
                    trade.get('timestamp'),
                    trade.get('symbol'),
                    trade.get('side'),
                    trade.get('price'),
                    trade.get('volume'),
                    trade.get('aggressor', True),
                    trade.get('order_id'),
                    trade.get('row')
                ))
            
            # Insere em batch
            cursor.executemany("""
                INSERT INTO trades (timestamp, symbol, side, price, volume, aggressor, order_id, row_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, trade_data)
            
            self.conn.commit()
            count = cursor.rowcount
            
            logger.debug(f"Salvos {count} trades no banco")
            return count
            
        except Exception as e:
            logger.error(f"Erro ao salvar trades: {e}")
            self.conn.rollback()
            return 0
            
    async def save_book_snapshot(self, symbol: str, book: Dict[str, Any]) -> bool:
        """Salva snapshot do book"""
        try:
            bids = book.get('bids', [])
            asks = book.get('asks', [])
            
            # Calcula spread e mid price
            spread = asks[0]['price'] - bids[0]['price'] if bids and asks else 0
            mid_price = (asks[0]['price'] + bids[0]['price']) / 2 if bids and asks else 0
            
            # Prepara dados (até 5 níveis)
            data = [datetime.now().isoformat(), symbol]
            
            # Adiciona bids
            for i in range(5):
                if i < len(bids):
                    data.extend([bids[i]['price'], bids[i]['volume']])
                else:
                    data.extend([None, None])
                    
            # Adiciona asks
            for i in range(5):
                if i < len(asks):
                    data.extend([asks[i]['price'], asks[i]['volume']])
                else:
                    data.extend([None, None])
                    
            data.extend([spread, mid_price])
            
            # Insere no banco
            self.conn.execute("""
                INSERT INTO book_snapshots (
                    timestamp, symbol,
                    bid_price_1, bid_volume_1, bid_price_2, bid_volume_2,
                    bid_price_3, bid_volume_3, bid_price_4, bid_volume_4,
                    bid_price_5, bid_volume_5,
                    ask_price_1, ask_volume_1, ask_price_2, ask_volume_2,
                    ask_price_3, ask_volume_3, ask_price_4, ask_volume_4,
                    ask_price_5, ask_volume_5,
                    spread, mid_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar book snapshot: {e}")
            self.conn.rollback()
            return False
            
    async def update_price_levels(self, symbol: str, price: float, volume: int, 
                                 level_type: Optional[str] = None) -> None:
        """Atualiza níveis de preço para análise de suporte/resistência"""
        try:
            # Se não especificado, tenta detectar o tipo
            if not level_type:
                level_type = await self._detect_level_type(symbol, price)
                
            cursor = self.conn.cursor()
            
            # Verifica se o nível já existe
            cursor.execute("""
                SELECT id, touch_count, volume_traded 
                FROM price_levels 
                WHERE symbol = ? AND price = ? AND level_type = ?
            """, (symbol, price, level_type))
            
            row = cursor.fetchone()
            
            if row:
                # Atualiza nível existente
                cursor.execute("""
                    UPDATE price_levels 
                    SET touch_count = touch_count + 1,
                        volume_traded = volume_traded + ?,
                        last_seen = CURRENT_TIMESTAMP,
                        strength = MIN(touch_count + 1, 10)
                    WHERE id = ?
                """, (volume, row['id']))
            else:
                # Cria novo nível
                cursor.execute("""
                    INSERT INTO price_levels (symbol, price, level_type, volume_traded)
                    VALUES (?, ?, ?, ?)
                """, (symbol, price, level_type, volume))
                
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Erro ao atualizar níveis de preço: {e}")
            self.conn.rollback()
            
    async def _detect_level_type(self, symbol: str, price: float) -> str:
        """Detecta se o preço é suporte ou resistência baseado no histórico"""
        # Implementação simplificada - pode ser melhorada
        cursor = self.conn.cursor()
        
        # Busca últimos 100 trades
        cursor.execute("""
            SELECT price FROM trades 
            WHERE symbol = ? 
            ORDER BY id DESC 
            LIMIT 100
        """, (symbol,))
        
        prices = [row['price'] for row in cursor.fetchall()]
        
        if not prices:
            return 'PIVOT'
            
        avg_price = sum(prices) / len(prices)
        
        if price > avg_price:
            return 'RESISTANCE'
        else:
            return 'SUPPORT'
            
    async def get_support_resistance_levels(self, symbol: str, min_strength: int = 3) -> Dict[str, List[Dict]]:
        """Retorna níveis de suporte e resistência significativos"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT price, level_type, strength, touch_count, volume_traded,
                   first_seen, last_seen
            FROM price_levels
            WHERE symbol = ? AND strength >= ?
            ORDER BY strength DESC, volume_traded DESC
        """, (symbol, min_strength))
        
        levels = {'support': [], 'resistance': [], 'pivot': []}
        
        for row in cursor.fetchall():
            level_data = {
                'price': row['price'],
                'strength': row['strength'],
                'touches': row['touch_count'],
                'volume': row['volume_traded'],
                'first_seen': row['first_seen'],
                'last_seen': row['last_seen']
            }
            
            level_type = row['level_type'].lower()
            if level_type in levels:
                levels[level_type].append(level_data)
                
        return levels
        
    async def validate_trades(self, expected_trades: List[Dict], 
                            actual_trades: List[Dict]) -> Dict[str, Any]:
        """Valida se todos os trades foram capturados corretamente"""
        validation_result = {
            'status': 'OK',
            'expected_count': len(expected_trades),
            'actual_count': len(actual_trades),
            'missing': [],
            'extra': [],
            'match_rate': 0.0
        }
        
        # Cria conjuntos de hashes para comparação
        expected_hashes = {self._hash_trade(t) for t in expected_trades}
        actual_hashes = {self._hash_trade(t) for t in actual_trades}
        
        # Encontra diferenças
        missing_hashes = expected_hashes - actual_hashes
        extra_hashes = actual_hashes - expected_hashes
        
        # Identifica trades específicos
        for trade in expected_trades:
            if self._hash_trade(trade) in missing_hashes:
                validation_result['missing'].append(trade)
                
        for trade in actual_trades:
            if self._hash_trade(trade) in extra_hashes:
                validation_result['extra'].append(trade)
                
        # Calcula taxa de correspondência
        if expected_trades:
            validation_result['match_rate'] = (
                len(expected_trades) - len(validation_result['missing'])
            ) / len(expected_trades) * 100
            
        # Define status
        if validation_result['missing']:
            validation_result['status'] = 'ERROR'
        elif validation_result['extra']:
            validation_result['status'] = 'WARNING'
            
        # Salva no log de validação
        await self._save_validation_log(validation_result)
        
        return validation_result
        
    def _hash_trade(self, trade: Dict) -> str:
        """Cria hash único para um trade"""
        key = f"{trade.get('timestamp')}_{trade.get('symbol')}_{trade.get('price')}_{trade.get('volume')}"
        return key
        
    async def _save_validation_log(self, result: Dict) -> None:
        """Salva resultado da validação no banco"""
        try:
            self.conn.execute("""
                INSERT INTO validation_log 
                (timestamp, check_type, expected_count, actual_count, 
                 missing_trades, extra_trades, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                'TRADE_VALIDATION',
                result['expected_count'],
                result['actual_count'],
                json.dumps(result['missing'][:10]),  # Salva até 10 para não sobrecarregar
                json.dumps(result['extra'][:10]),
                result['status']
            ))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Erro ao salvar log de validação: {e}")
            
    async def get_market_stats(self, symbol: str, date: Optional[date] = None) -> List[Dict]:
        """Retorna estatísticas agregadas do mercado"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT * FROM market_stats
                WHERE symbol = ? AND date = ?
                ORDER BY hour
            """, (symbol, date))
        else:
            cursor.execute("""
                SELECT * FROM market_stats
                WHERE symbol = ? AND date = date('now')
                ORDER BY hour
            """, (symbol,))
            
        return [dict(row) for row in cursor.fetchall()]
        
    async def update_market_stats(self, trades: List[Dict]) -> None:
        """Atualiza estatísticas agregadas com novos trades"""
        if not trades:
            return
            
        try:
            # Agrupa por símbolo e hora
            from collections import defaultdict
            stats = defaultdict(lambda: {
                'trades': 0, 'volume': 0, 'buy_trades': 0, 'sell_trades': 0,
                'buy_volume': 0, 'sell_volume': 0, 'prices': []
            })
            
            for trade in trades:
                # Extrai hora do timestamp
                ts = trade.get('timestamp', '')
                hour = int(ts.split(':')[0]) if ':' in ts else 0
                key = (trade['symbol'], date.today(), hour)
                
                stat = stats[key]
                stat['trades'] += 1
                stat['volume'] += trade['volume']
                stat['prices'].append(trade['price'])
                
                if trade['side'] == 'BUY':
                    stat['buy_trades'] += 1
                    stat['buy_volume'] += trade['volume']
                else:
                    stat['sell_trades'] += 1
                    stat['sell_volume'] += trade['volume']
                    
            # Salva no banco
            for (symbol, dt, hour), stat in stats.items():
                prices = stat['prices']
                vwap = sum(prices) / len(prices) if prices else 0
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO market_stats
                    (symbol, date, hour, total_trades, total_volume,
                     buy_trades, sell_trades, buy_volume, sell_volume,
                     high_price, low_price, vwap)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, dt, hour, stat['trades'], stat['volume'],
                    stat['buy_trades'], stat['sell_trades'],
                    stat['buy_volume'], stat['sell_volume'],
                    max(prices) if prices else None,
                    min(prices) if prices else None,
                    vwap
                ))
                
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Erro ao atualizar estatísticas: {e}")
            self.conn.rollback()
            
    async def close(self):
        """Fecha conexão com o banco"""
        if self.conn:
            self.conn.close()
            logger.info("Conexão com banco de dados fechada")