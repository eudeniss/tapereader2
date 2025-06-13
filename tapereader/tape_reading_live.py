#!/usr/bin/env python
"""
Sistema de Tape Reading com Detecção de Padrões
Detecta Absorção e Exaustão em tempo real
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.config import ConfigManager
from src.core.cache import CacheManager
from src.data.excel_provider import ExcelDataProvider
from src.analysis.analyzers import VolumeAnalyzer, PriceActionAnalyzer, OrderFlowAnalyzer, ImbalanceAnalyzer
from src.behaviors.absorption import AbsorptionDetector
from src.behaviors.exhaustion import ExhaustionDetector
from src.core.models import Symbol

# Cores para terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    BLINK = '\033[5m'

class TapeReadingSystem:
    """Sistema principal de tape reading"""
    
    def __init__(self, config_manager, data_provider):
        self.config_manager = config_manager
        self.data_provider = data_provider
        
        # Analisadores
        self.volume_analyzer = VolumeAnalyzer({})
        self.price_analyzer = PriceActionAnalyzer({})
        self.flow_analyzer = OrderFlowAnalyzer({})
        self.imbalance_analyzer = ImbalanceAnalyzer({})
        
        # Detectores de comportamento
        absorption_config = {
            'volume_threshold': 500,     # Volume mínimo para absorção
            'price_impact_max': 0.0002,  # Máximo 0.02% de movimento
            'time_window': 60            # Janela de 60 segundos
        }
        
        exhaustion_config = {
            'momentum_decay_rate': 0.7,  # 70% de decaimento
            'volume_decay_rate': 0.6,    # 60% de decaimento  
            'confirmation_bars': 3       # 3 barras de confirmação
        }
        
        self.absorption_detector = AbsorptionDetector(absorption_config)
        self.exhaustion_detector = ExhaustionDetector(exhaustion_config)
        
        # Histórico e alertas
        self.trade_history = deque(maxlen=1000)
        self.behavior_alerts = deque(maxlen=20)
        self.active_signals = []
        
        # Estatísticas
        self.stats = {
            'behaviors_detected': 0,
            'absorption_count': 0,
            'exhaustion_count': 0,
            'trades_analyzed': 0
        }
    
    async def analyze_market(self, trades: List[Dict], symbol: Symbol) -> Dict[str, Any]:
        """Analisa mercado e detecta comportamentos"""
        if not trades:
            return {}
        
        # Análises básicas
        volume_analysis = await self.volume_analyzer.analyze(trades)
        price_analysis = await self.price_analyzer.analyze(trades)
        flow_analysis = await self.flow_analyzer.analyze(trades)
        
        # Detecção de comportamentos
        behaviors = []
        
        # Verifica absorção
        absorption = await self.absorption_detector.detect(trades, symbol)
        if absorption:
            behaviors.append(absorption)
            self.stats['absorption_count'] += 1
            self.behavior_alerts.append({
                'time': datetime.now(),
                'type': 'ABSORPTION',
                'symbol': symbol,
                'confidence': absorption.confidence,
                'details': absorption.metadata
            })
        
        # Verifica exaustão
        exhaustion = await self.exhaustion_detector.detect(trades, symbol)
        if exhaustion:
            behaviors.append(exhaustion)
            self.stats['exhaustion_count'] += 1
            self.behavior_alerts.append({
                'time': datetime.now(),
                'type': 'EXHAUSTION',
                'symbol': symbol,
                'confidence': exhaustion.confidence,
                'details': exhaustion.metadata
            })
        
        self.stats['behaviors_detected'] = len(behaviors)
        
        return {
            'volume': volume_analysis,
            'price': price_analysis,
            'flow': flow_analysis,
            'behaviors': behaviors,
            'alerts': list(self.behavior_alerts)[-5:]  # Últimos 5 alertas
        }
    
    def generate_signals(self, analysis: Dict[str, Any]) -> List[Dict]:
        """Gera sinais baseados na análise"""
        signals = []
        
        for behavior in analysis.get('behaviors', []):
            signal = None
            
            if behavior.type.value == 'absorption':
                # Absorção pode indicar acumulação/distribuição
                flow_bias = analysis['flow'].get('flow_bias', 'NEUTRAL')
                
                if flow_bias == 'BULLISH' and behavior.confidence > 0.7:
                    signal = {
                        'type': 'BUY',
                        'reason': 'Absorção de venda detectada',
                        'confidence': behavior.confidence,
                        'symbol': behavior.symbol,
                        'entry': analysis['price']['current_price'],
                        'stop': analysis['price']['low'],
                        'target': analysis['price']['current_price'] + (analysis['price']['range'] * 2)
                    }
                elif flow_bias == 'BEARISH' and behavior.confidence > 0.7:
                    signal = {
                        'type': 'SELL',
                        'reason': 'Absorção de compra detectada',
                        'confidence': behavior.confidence,
                        'symbol': behavior.symbol,
                        'entry': analysis['price']['current_price'],
                        'stop': analysis['price']['high'],
                        'target': analysis['price']['current_price'] - (analysis['price']['range'] * 2)
                    }
            
            elif behavior.type.value == 'exhaustion':
                # Exaustão indica possível reversão
                direction = behavior.metadata.get('direction', 'NEUTRAL')
                
                if direction == 'BULLISH' and behavior.confidence > 0.75:
                    signal = {
                        'type': 'SELL',
                        'reason': 'Exaustão de alta detectada',
                        'confidence': behavior.confidence,
                        'symbol': behavior.symbol,
                        'entry': analysis['price']['current_price'],
                        'stop': analysis['price']['high'] + 2,
                        'target': analysis['price']['average']
                    }
                elif direction == 'BEARISH' and behavior.confidence > 0.75:
                    signal = {
                        'type': 'BUY',
                        'reason': 'Exaustão de baixa detectada',
                        'confidence': behavior.confidence,
                        'symbol': behavior.symbol,
                        'entry': analysis['price']['current_price'],
                        'stop': analysis['price']['low'] - 2,
                        'target': analysis['price']['average']
                    }
            
            if signal:
                signal['timestamp'] = datetime.now()
                signal['risk_reward'] = abs(signal['target'] - signal['entry']) / abs(signal['stop'] - signal['entry'])
                signals.append(signal)
                self.active_signals.append(signal)
        
        return signals

async def run_tape_reading():
    """Executa sistema de tape reading"""
    print(f"{Colors.BOLD}=== SISTEMA DE TAPE READING ATIVO ==={Colors.RESET}")
    print(f"Detectando: {Colors.CYAN}ABSORÇÃO{Colors.RESET} e {Colors.MAGENTA}EXAUSTÃO{Colors.RESET}\n")
    
    try:
        # Inicialização
        config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
        config_manager = ConfigManager(config_dir=config_dir, env='production')
        cache_manager = CacheManager(config_manager.get('cache', {}))
        await cache_manager.initialize()
        
        excel_config = config_manager.get_provider_config('excel')
        data_provider = ExcelDataProvider(excel_config, cache_manager)
        await data_provider.initialize()
        
        # Sistema de tape reading
        tape_system = TapeReadingSystem(config_manager, data_provider)
        
        print(f"{Colors.GREEN}✅ Sistema inicializado{Colors.RESET}")
        print("Analisando fluxo... (Ctrl+C para parar)\n")
        
        # Buffer de trades por símbolo
        wdo_buffer = deque(maxlen=200)
        dol_buffer = deque(maxlen=200)
        
        while True:
            # Obtém dados
            data = await data_provider.get_data()
            
            if data and data.get('trades'):
                # Separa trades por símbolo
                for trade in data['trades']:
                    tape_system.stats['trades_analyzed'] += 1
                    
                    if trade['symbol'] == 'WDOFUT':
                        wdo_buffer.append(trade)
                    else:
                        dol_buffer.append(trade)
                
                # Limpa tela
                os.system('cls' if os.name == 'nt' else 'clear')
                
                print(f"{Colors.BOLD}=== SISTEMA DE TAPE READING ATIVO ==={Colors.RESET}")
                print(f"Horário: {datetime.now().strftime('%H:%M:%S')}")
                print("=" * 120)
                
                # Analisa cada símbolo
                analyses = {}
                
                if len(wdo_buffer) > 10:
                    analyses['WDOFUT'] = await tape_system.analyze_market(list(wdo_buffer), Symbol.WDOFUT)
                
                if len(dol_buffer) > 10:
                    analyses['DOLFUT'] = await tape_system.analyze_market(list(dol_buffer), Symbol.DOLFUT)
                
                # SEÇÃO 1: ALERTAS DE COMPORTAMENTO
                if tape_system.behavior_alerts:
                    print(f"\n{Colors.BLINK}🚨 ALERTAS DE COMPORTAMENTO{Colors.RESET}")
                    print("-" * 120)
                    
                    for alert in list(tape_system.behavior_alerts)[-5:]:
                        time_str = alert['time'].strftime('%H:%M:%S')
                        
                        if alert['type'] == 'ABSORPTION':
                            icon = "🧲"
                            color = Colors.CYAN
                            details = f"Volume: {alert['details'].get('volume', 0):,} | Impact: {alert['details'].get('price_impact', 0):.4f}"
                        else:  # EXHAUSTION
                            icon = "⚡"
                            color = Colors.MAGENTA
                            direction = alert['details'].get('direction', 'NEUTRAL')
                            details = f"Direção: {direction} | Momentum: {alert['details'].get('momentum_score', 0):.2f}"
                        
                        print(f"{time_str} {icon} {color}{alert['type']}{Colors.RESET} em {alert['symbol']} "
                              f"[Confiança: {alert['confidence']:.1%}] {details}")
                
                # SEÇÃO 2: ANÁLISE POR SÍMBOLO
                print(f"\n{Colors.BOLD}📊 ANÁLISE DE FLUXO{Colors.RESET}")
                print("-" * 120)
                
                for symbol, analysis in analyses.items():
                    if not analysis:
                        continue
                    
                    symbol_color = Colors.CYAN if symbol == 'WDOFUT' else Colors.MAGENTA
                    print(f"\n{symbol_color}{symbol}{Colors.RESET}:")
                    
                    # Fluxo
                    flow = analysis.get('flow', {})
                    if flow:
                        bias_color = Colors.GREEN if flow.get('flow_bias') == 'BULLISH' else Colors.RED if flow.get('flow_bias') == 'BEARISH' else Colors.YELLOW
                        print(f"  Fluxo: {bias_color}{flow.get('flow_bias', 'NEUTRAL')}{Colors.RESET} | "
                              f"Delta: {flow.get('volume_delta', 0):+,} ({flow.get('delta_percent', 0):+.1f}%)")
                    
                    # Volume
                    volume = analysis.get('volume', {})
                    if volume:
                        vol_trend = volume.get('volume_trend', 'NEUTRAL')
                        vol_color = Colors.GREEN if vol_trend == 'INCREASING' else Colors.RED if vol_trend == 'DECREASING' else Colors.YELLOW
                        print(f"  Volume: {vol_color}{vol_trend}{Colors.RESET} | "
                              f"Total: {volume.get('current_volume', 0):,}")
                    
                    # Comportamentos detectados
                    behaviors = analysis.get('behaviors', [])
                    if behaviors:
                        print(f"  {Colors.BOLD}Comportamentos:{Colors.RESET}")
                        for behavior in behaviors:
                            print(f"    - {behavior.type.value.upper()} (confiança: {behavior.confidence:.1%})")
                
                # SEÇÃO 3: SINAIS GERADOS
                all_signals = []
                for symbol, analysis in analyses.items():
                    signals = tape_system.generate_signals(analysis)
                    all_signals.extend(signals)
                
                if all_signals:
                    print(f"\n{Colors.BOLD}💡 SINAIS DE TRADING{Colors.RESET}")
                    print("-" * 120)
                    
                    for signal in all_signals[-5:]:  # Últimos 5 sinais
                        signal_color = Colors.GREEN if signal['type'] == 'BUY' else Colors.RED
                        
                        print(f"\n{signal_color}▶ {signal['type']}{Colors.RESET} {signal['symbol']} - {signal['reason']}")
                        print(f"  Entry: {signal['entry']:.1f} | Stop: {signal['stop']:.1f} | Target: {signal['target']:.1f}")
                        print(f"  R:R: {signal['risk_reward']:.1f}:1 | Confiança: {signal['confidence']:.1%}")
                
                # SEÇÃO 4: ESTATÍSTICAS
                print(f"\n{Colors.BOLD}📈 ESTATÍSTICAS{Colors.RESET}")
                print("-" * 120)
                print(f"Trades Analisados: {tape_system.stats['trades_analyzed']:,}")
                print(f"Absorções Detectadas: {tape_system.stats['absorption_count']}")
                print(f"Exaustões Detectadas: {tape_system.stats['exhaustion_count']}")
                
                if tape_system.active_signals:
                    print(f"\nSinais Ativos: {len(tape_system.active_signals)}")
                    
                    # Performance dos sinais
                    profitable = sum(1 for s in tape_system.active_signals 
                                   if s.get('status') == 'PROFITABLE')
                    if len(tape_system.active_signals) > 0:
                        win_rate = profitable / len(tape_system.active_signals) * 100
                        print(f"Taxa de Acerto: {win_rate:.1f}%")
                
                print("=" * 120)
            
            await asyncio.sleep(0.5)
            
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Sistema interrompido{Colors.RESET}")
    except Exception as e:
        print(f"\n{Colors.RED}❌ Erro: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
    finally:
        if 'data_provider' in locals():
            await data_provider.close()
        print("\nSistema encerrado.")

if __name__ == "__main__":
    asyncio.run(run_tape_reading())