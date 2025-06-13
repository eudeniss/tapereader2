import logging
import xlwings as xw
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import hashlib
from .provider import DataProvider

logger = logging.getLogger(__name__)

class ExcelDataProvider(DataProvider):
    """Provider de dados via Excel RTD com ordenação cronológica correta"""
    
    def __init__(self, config: Dict[str, Any], cache_manager: Any):
        super().__init__(config, cache_manager)
        self.wb = None
        self.ws = None
        self.first_run = True
        self.read_count = 0
        
        # Sistema simplificado - usa timestamp nativo com ms
        self.processed_trades = set()  # Conjunto de hashes únicos
        self.last_excel_state = {}  # linha -> hash do trade para detecção rápida
        
        logger.info(f"ExcelDataProvider inicializado")
        logger.info(f"Config recebido: {config}")
        
    async def initialize(self):
        """Inicializa conexao com Excel"""
        try:
            self.wb = xw.Book(self.config.get('file_path', 'rtd_tapeReading.xlsx'))
            self.ws = self.wb.sheets[self.config.get('sheet_name', 'Sheet1')]
            logger.info(f"Conectado ao Excel: {self.wb.name}")
            logger.info(f"Planilha ativa: {self.ws.name}")
            logger.info(f"Células usadas: {self.ws.used_range.address}")
        except Exception as e:
            logger.error(f"Erro ao conectar ao Excel: {e}")
            raise
    
    def _create_trade_hash(self, trade: Dict) -> str:
        """
        Cria hash único do trade usando timestamp COM milissegundos
        Isso garante que trades idênticos no mesmo milissegundo sejam únicos
        """
        # Usa o timestamp completo com milissegundos
        trade_str = (
            f"{trade.get('timestamp', '')}_"  # Já inclui ms: 09:23:58.004
            f"{trade.get('symbol', '')}_"
            f"{trade.get('side', '')}_"
            f"{trade.get('price', 0)}_"
            f"{trade.get('volume', 0)}"
        )
        return hashlib.md5(trade_str.encode()).hexdigest()
    
    def _parse_timestamp_with_ms(self, timestamp_str: str) -> str:
        """
        Garante que o timestamp está no formato correto com milissegundos
        Entrada: "09:23:58.004" ou "09:23:58"
        Saída: "2025-01-13 09:23:58.004"
        """
        try:
            # Remove espaços
            timestamp_str = str(timestamp_str).strip()
            
            # Se já tem data completa
            if len(timestamp_str) > 12:  # "2025-01-13 09:23:58.004"
                return timestamp_str
            
            # Se é só hora (com ou sem ms)
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Se não tem milissegundos, adiciona .000
            if '.' not in timestamp_str:
                timestamp_str += '.000'
            
            return f"{today} {timestamp_str}"
            
        except Exception as e:
            logger.debug(f"Erro ao parsear timestamp: {e}")
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    def _parse_time_for_sorting(self, timestamp_str: str) -> datetime:
        """
        Converte timestamp string para datetime para ordenação
        """
        try:
            # Remove espaços
            timestamp_str = str(timestamp_str).strip()
            
            # Se é só hora HH:MM:SS.mmm
            if len(timestamp_str) <= 12:
                today = datetime.now().date()
                # Parse do tempo
                if '.' in timestamp_str:
                    # Com milissegundos
                    time_parts = timestamp_str.split('.')
                    time_obj = datetime.strptime(time_parts[0], "%H:%M:%S").time()
                    milliseconds = int(time_parts[1].ljust(3, '0')[:3])
                    return datetime.combine(today, time_obj).replace(microsecond=milliseconds * 1000)
                else:
                    # Sem milissegundos
                    time_obj = datetime.strptime(timestamp_str, "%H:%M:%S").time()
                    return datetime.combine(today, time_obj)
            else:
                # Timestamp completo
                return datetime.strptime(timestamp_str[:23], "%Y-%m-%d %H:%M:%S.%f")
                
        except Exception as e:
            logger.debug(f"Erro ao parsear tempo para ordenação: {e}")
            return datetime.now()
    
    async def get_data(self) -> Optional[Dict[str, Any]]:
        """Le dados do Excel com detecção inteligente e ordenação cronológica"""
        self.read_count += 1
        
        if self.read_count % 10 == 0:
            logger.info(f"Leituras realizadas: {self.read_count}")
            logger.info(f"Trades processados: {len(self.processed_trades)}")
        
        try:
            # Lê dados atuais
            all_current_data = {
                'timestamp': datetime.now(),
                'wdofut': {
                    'trades': self._read_trades('wdofut_trades'),
                    'book': self._read_book('wdofut_book')
                },
                'dolfut': {
                    'trades': self._read_trades('dolfut_trades'),
                    'book': self._read_book('dolfut_book')
                }
            }
            
            # Na primeira execução
            if self.first_run:
                self.first_run = False
                logger.info("Primeira execução - processando trades existentes")
                
                initial_trades = []
                
                for symbol in ['wdofut', 'dolfut']:
                    trades = all_current_data[symbol]['trades']
                    
                    # IMPORTANTE: Ordena trades do mais antigo para o mais novo
                    trades_sorted = sorted(trades, key=lambda t: self._parse_time_for_sorting(t['timestamp']))
                    
                    # Processa apenas os primeiros 30 trades (mais antigos)
                    for trade in trades_sorted[:30]:
                        trade_hash = self._create_trade_hash(trade)
                        
                        if trade_hash not in self.processed_trades:
                            self.processed_trades.add(trade_hash)
                            initial_trades.append(trade)
                            
                            # Salva estado por linha para detecção rápida
                            row_key = f"{symbol}_{trade.get('row', 0)}"
                            self.last_excel_state[row_key] = trade_hash
                            
                            logger.debug(f"Trade inicial: {trade['timestamp']} | "
                                       f"{trade['symbol']} | {trade['side']} | "
                                       f"{trade['price']} | {trade['volume']}")
                
                # Ordena trades finais cronologicamente antes de retornar
                initial_trades_sorted = sorted(initial_trades, 
                                             key=lambda t: self._parse_time_for_sorting(t['timestamp']))
                
                logger.info(f"Inicializado com {len(initial_trades_sorted)} trades em ordem cronológica")
                
                if initial_trades_sorted:
                    return {
                        'trades': initial_trades_sorted,
                        'book': self._merge_books(all_current_data),
                        'timestamp': all_current_data['timestamp']
                    }
                    
            else:
                # Detecta novos trades
                new_trades = []
                
                for symbol in ['wdofut', 'dolfut']:
                    current_trades = all_current_data[symbol]['trades']
                    
                    # Verifica todos os trades (não apenas primeiras linhas)
                    for trade in current_trades:
                        trade_hash = self._create_trade_hash(trade)
                        row_key = f"{symbol}_{trade.get('row', 0)}"
                        
                        # É novo se:
                        # 1. Hash nunca foi visto (novo trade único)
                        # 2. Linha tem hash diferente (novo trade substituiu antigo)
                        
                        is_new = False
                        
                        if trade_hash not in self.processed_trades:
                            # Trade completamente novo
                            is_new = True
                            self.processed_trades.add(trade_hash)
                            
                        # Atualiza estado da linha
                        old_hash = self.last_excel_state.get(row_key)
                        if old_hash != trade_hash:
                            self.last_excel_state[row_key] = trade_hash
                            if not is_new and old_hash is not None:
                                # Trade mudou nesta linha mas já foi processado
                                # (dados desceram)
                                logger.debug(f"Trade moveu de linha: {trade['timestamp']}")
                        
                        if is_new:
                            new_trades.append(trade)
                            logger.info(f"NOVO TRADE: {trade['timestamp']} | "
                                      f"{trade['symbol']} | {trade['side']} | "
                                      f"{trade['price']} | {trade['volume']}")
                
                # Limpeza periódica de memória
                if len(self.processed_trades) > 20000:
                    logger.info("Limpando memória...")
                    # Remove hashes mais antigos
                    trades_list = list(self.processed_trades)
                    self.processed_trades = set(trades_list[-10000:])
                    logger.info(f"Memória limpa. Trades em cache: {len(self.processed_trades)}")
                
                if new_trades:
                    # IMPORTANTE: Ordena novos trades cronologicamente
                    new_trades_sorted = sorted(new_trades, 
                                             key=lambda t: self._parse_time_for_sorting(t['timestamp']))
                    
                    logger.info(f"{len(new_trades_sorted)} novos trades detectados (ordenados cronologicamente)")
                    return {
                        'trades': new_trades_sorted,
                        'book': self._merge_books(all_current_data),
                        'timestamp': all_current_data['timestamp']
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao ler dados: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _merge_books(self, data: Dict) -> Dict:
        """Mescla books de WDOFUT e DOLFUT"""
        dol_book = data['dolfut']['book']
        wdo_book = data['wdofut']['book']
        
        return {
            'bids': dol_book['bids'] if dol_book['bids'] else wdo_book['bids'],
            'asks': dol_book['asks'] if dol_book['asks'] else wdo_book['asks']
        }
    
    def _read_trades(self, range_key: str) -> List[Dict]:
        """Le trades do Excel preservando milissegundos nativos"""
        if self.read_count <= 1:
            logger.debug(f"=== Lendo trades: {range_key} ===")
        
        ranges = self.config.get('ranges', {})
        if not ranges:
            logger.warning(f"Nenhum range encontrado no config!")
            return []
            
        range_config = ranges.get(range_key, {})
        if not range_config:
            logger.warning(f"Configuração para '{range_key}' não encontrada!")
            return []
            
        trades = []
        
        try:
            start_row = range_config.get('start_row', 3)
            max_rows = range_config.get('max_rows', 100)
            
            # Colunas configuradas
            data_col = range_config.get('data', 'B')
            agressao_col = range_config.get('agressao', 'C')
            valor_col = range_config.get('valor', 'D')
            qtde_col = range_config.get('quantidade', 'E')
            
            # Símbolo baseado no range
            symbol = 'WDOFUT' if 'wdofut' in range_key else 'DOLFUT'
            
            # Le linha por linha
            for row_num in range(start_row, start_row + max_rows):
                try:
                    # Le células
                    data_val = self.ws.range(f"{data_col}{row_num}").value
                    agressao_val = self.ws.range(f"{agressao_col}{row_num}").value
                    valor_val = self.ws.range(f"{valor_col}{row_num}").value
                    qtde_val = self.ws.range(f"{qtde_col}{row_num}").value
                    
                    # Pula se não tem dados
                    if not data_val or not valor_val or not qtde_val:
                        continue
                    
                    # Converte valores
                    try:
                        # O timestamp já vem com milissegundos: "09:23:58.004"
                        timestamp_str = str(data_val).strip()
                        
                        # Parse do timestamp preservando ms
                        full_timestamp = self._parse_timestamp_with_ms(timestamp_str)
                        
                        # Converte preço e volume
                        price = float(str(valor_val).replace(',', '.'))
                        volume = int(float(str(qtde_val)))
                        
                        # Determina lado
                        side = 'BUY' if agressao_val == 'Comprador' else 'SELL'
                        
                        # Cria trade com timestamp completo
                        trade = {
                            'timestamp': timestamp_str,  # Preserva ms original: "09:23:58.004"
                            'timestamp_full': full_timestamp,  # Com data: "2025-01-13 09:23:58.004"
                            'order_id': f"{symbol}_{timestamp_str}_{row_num}",
                            'symbol': symbol,
                            'side': side,
                            'price': price,
                            'volume': volume,
                            'aggressor': True,
                            'row': row_num
                        }
                        
                        trades.append(trade)
                        
                        # Log dos primeiros trades para debug
                        if self.read_count <= 1 and len(trades) <= 3:
                            logger.debug(f"Trade lido: {timestamp_str} | {symbol} | "
                                       f"{side} | {price} | {volume}")
                            
                    except (ValueError, TypeError) as e:
                        if self.read_count <= 1:
                            logger.debug(f"Erro ao converter linha {row_num}: {e}")
                        continue
                        
                except Exception as e:
                    if self.read_count <= 1:
                        logger.debug(f"Erro ao ler linha {row_num}: {e}")
                    continue
            
            if self.read_count <= 1 and len(trades) > 0:
                logger.info(f"{len(trades)} trades lidos para {range_key}")
                # Mostra ordem dos trades lidos
                if trades:
                    logger.info(f"   Primeiro da lista (linha {trades[0]['row']}): {trades[0]['timestamp']}")
                    logger.info(f"   Último da lista (linha {trades[-1]['row']}): {trades[-1]['timestamp']}")
            
        except Exception as e:
            logger.error(f"Erro ao ler trades {range_key}: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        return trades
    
    def _read_book(self, range_key: str) -> Dict:
        """Le book de ofertas"""
        ranges = self.config.get('ranges', {})
        if not ranges:
            return {'bids': [], 'asks': []}
            
        range_config = ranges.get(range_key, {})
        if not range_config:
            return {'bids': [], 'asks': []}
            
        book = {'bids': [], 'asks': []}
        
        try:
            start_row = range_config.get('start_row', 3)
            max_rows = range_config.get('max_rows', 20)
            
            # Colunas do book
            qtde_compra_col = range_config.get('qtde_compra', 'N')
            compra_col = range_config.get('compra', 'O')
            venda_col = range_config.get('venda', 'P')
            qtde_venda_col = range_config.get('qtde_venda', 'Q')
            
            # Le linha por linha
            for row_num in range(start_row, start_row + max_rows):
                try:
                    qtde_compra = self.ws.range(f"{qtde_compra_col}{row_num}").value
                    compra = self.ws.range(f"{compra_col}{row_num}").value
                    venda = self.ws.range(f"{venda_col}{row_num}").value
                    qtde_venda = self.ws.range(f"{qtde_venda_col}{row_num}").value
                    
                    # Processa bid
                    if compra and qtde_compra:
                        try:
                            price = float(str(compra).replace(',', '.'))
                            volume = int(float(str(qtde_compra)))
                            
                            if price > 0 and volume > 0:
                                book['bids'].append({
                                    'price': price,
                                    'volume': volume
                                })
                        except (ValueError, TypeError):
                            pass
                    
                    # Processa ask
                    if venda and qtde_venda:
                        try:
                            price = float(str(venda).replace(',', '.'))
                            volume = int(float(str(qtde_venda)))
                            
                            if price > 0 and volume > 0:
                                book['asks'].append({
                                    'price': price,
                                    'volume': volume
                                })
                        except (ValueError, TypeError):
                            pass
                            
                except Exception as e:
                    logger.debug(f"Erro ao ler linha {row_num} do book: {e}")
                    continue
            
            # Ordena o book
            book['bids'].sort(key=lambda x: x['price'], reverse=True)
            book['asks'].sort(key=lambda x: x['price'])
            
        except Exception as e:
            logger.error(f"Erro ao ler book {range_key}: {e}")
        
        return book
    
    def _debug_check_cells(self):
        """Verifica células para debug - mostra timestamps com ms"""
        logger.info("=== DEBUG: Verificando timestamps com milissegundos ===")
        
        ranges = self.config.get('ranges', {})
        
        # Verifica WDOFUT
        wdo_config = ranges.get('wdofut_trades', {})
        if wdo_config:
            logger.info("=== WDOFUT - Timestamps ===")
            data_col = wdo_config.get('data', 'B')
            start_row = wdo_config.get('start_row', 4)
            
            for row in range(start_row, start_row + 5):
                timestamp = self.ws.range(f'{data_col}{row}').value
                if timestamp:
                    logger.info(f"Linha {row}: {timestamp}")
        
        # Verifica DOLFUT
        dol_config = ranges.get('dolfut_trades', {})
        if dol_config:
            logger.info("=== DOLFUT - Timestamps ===")
            data_col = dol_config.get('data', 'H')
            start_row = dol_config.get('start_row', 4)
            
            for row in range(start_row, start_row + 5):
                timestamp = self.ws.range(f'{data_col}{row}').value
                if timestamp:
                    logger.info(f"Linha {row}: {timestamp}")
    
    async def close(self):
        """Fecha conexao com Excel"""
        if self.wb:
            logger.info("Fechando conexao com Excel")
            logger.info(f"Total de trades processados: {len(self.processed_trades)}")
            logger.info("Sistema encerrado com sucesso!")