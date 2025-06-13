#!/usr/bin/env python
"""
Visualizador de dados do banco SQLite
"""
import sqlite3
import os
from datetime import datetime, timedelta
from tabulate import tabulate  # pip install tabulate

# Cores para terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

class DatabaseViewer:
    def __init__(self, db_path="data/tape_reader.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
    def view_recent_trades(self, limit=20):
        """Mostra trades recentes"""
        print(f"\n{Colors.BOLD}=== ÚLTIMOS {limit} TRADES ==={Colors.RESET}\n")
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, timestamp, symbol, side, price, volume, created_at
            FROM trades
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        
        trades = cursor.fetchall()
        
        if trades:
            # Prepara dados para tabela
            table_data = []
            for trade in trades:
                side_color = Colors.GREEN if trade['side'] == 'BUY' else Colors.RED
                row = [
                    trade['id'],
                    trade['timestamp'],
                    trade['symbol'],
                    f"{side_color}{trade['side']}{Colors.RESET}",
                    f"${trade['price']:.1f}",
                    trade['volume'],
                    trade['created_at'][:19]  # Remove microsegundos
                ]
                table_data.append(row)
            
            headers = ['ID', 'Hora', 'Símbolo', 'Lado', 'Preço', 'Volume', 'Salvo em']
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            print("Nenhum trade encontrado!")
            
    def view_volume_profile(self, symbol='WDOFUT', limit=15):
        """Mostra perfil de volume por preço"""
        print(f"\n{Colors.BOLD}=== PERFIL DE VOLUME - {symbol} ==={Colors.RESET}\n")
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                price,
                SUM(CASE WHEN side = 'BUY' THEN volume ELSE 0 END) as buy_volume,
                SUM(CASE WHEN side = 'SELL' THEN volume ELSE 0 END) as sell_volume,
                SUM(volume) as total_volume,
                COUNT(*) as trade_count
            FROM trades
            WHERE symbol = ?
            GROUP BY price
            ORDER BY total_volume DESC
            LIMIT ?
        """, (symbol, limit))
        
        levels = cursor.fetchall()
        
        if levels:
            max_volume = max(level['total_volume'] for level in levels)
            
            for level in levels:
                price = level['price']
                buy_vol = level['buy_volume']
                sell_vol = level['sell_volume']
                total_vol = level['total_volume']
                
                # Barra visual
                bar_size = int((total_vol / max_volume) * 40)
                buy_bar_size = int((buy_vol / total_vol) * bar_size)
                sell_bar_size = bar_size - buy_bar_size
                
                bar = f"{Colors.GREEN}{'█' * buy_bar_size}{Colors.RED}{'█' * sell_bar_size}{Colors.RESET}"
                
                print(f"${price:7.1f} | {bar} {total_vol:6d} ({level['trade_count']} trades)")
                
    def view_support_resistance(self, symbol='WDOFUT'):
        """Mostra níveis de suporte e resistência"""
        print(f"\n{Colors.BOLD}=== NÍVEIS DE SUPORTE/RESISTÊNCIA - {symbol} ==={Colors.RESET}\n")
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT price, level_type, strength, touch_count, volume_traded,
                   first_seen, last_seen
            FROM price_levels
            WHERE symbol = ? AND strength >= 2
            ORDER BY strength DESC, volume_traded DESC
            LIMIT 20
        """, (symbol,))
        
        levels = cursor.fetchall()
        
        if levels:
            # Separa por tipo
            supports = [l for l in levels if l['level_type'] == 'SUPPORT']
            resistances = [l for l in levels if l['level_type'] == 'RESISTANCE']
            pivots = [l for l in levels if l['level_type'] == 'PIVOT']
            
            # Mostra suportes
            if supports:
                print(f"{Colors.GREEN}📉 SUPORTES:{Colors.RESET}")
                for level in supports[:5]:
                    strength_bar = '▰' * level['strength'] + '▱' * (10 - level['strength'])
                    print(f"  ${level['price']:7.1f} | Força: {strength_bar} | "
                          f"Toques: {level['touch_count']:3d} | Volume: {level['volume_traded']:,}")
            
            # Mostra resistências
            if resistances:
                print(f"\n{Colors.RED}📈 RESISTÊNCIAS:{Colors.RESET}")
                for level in resistances[:5]:
                    strength_bar = '▰' * level['strength'] + '▱' * (10 - level['strength'])
                    print(f"  ${level['price']:7.1f} | Força: {strength_bar} | "
                          f"Toques: {level['touch_count']:3d} | Volume: {level['volume_traded']:,}")
                          
    def view_book_snapshots(self, symbol='WDOFUT', limit=10):
        """Mostra snapshots recentes do book"""
        print(f"\n{Colors.BOLD}=== BOOK SNAPSHOTS - {symbol} ==={Colors.RESET}\n")
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT timestamp, 
                   bid_price_1, bid_volume_1, 
                   ask_price_1, ask_volume_1,
                   spread, mid_price
            FROM book_snapshots
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT ?
        """, (symbol, limit))
        
        snapshots = cursor.fetchall()
        
        if snapshots:
            table_data = []
            for snap in snapshots:
                spread_color = Colors.GREEN if snap['spread'] < 1.0 else Colors.YELLOW
                row = [
                    snap['timestamp'][:19],
                    f"${snap['bid_price_1']:.1f} ({snap['bid_volume_1']})",
                    f"${snap['ask_price_1']:.1f} ({snap['ask_volume_1']})",
                    f"{spread_color}${snap['spread']:.1f}{Colors.RESET}",
                    f"${snap['mid_price']:.1f}"
                ]
                table_data.append(row)
                
            headers = ['Timestamp', 'Melhor Bid', 'Melhor Ask', 'Spread', 'Mid Price']
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
            
    def view_statistics(self):
        """Mostra estatísticas gerais"""
        print(f"\n{Colors.BOLD}=== ESTATÍSTICAS DO BANCO ==={Colors.RESET}\n")
        
        cursor = self.conn.cursor()
        
        # Total de trades
        cursor.execute("SELECT COUNT(*) as total, MIN(created_at) as first, MAX(created_at) as last FROM trades")
        trade_stats = cursor.fetchone()
        
        # Por símbolo
        cursor.execute("""
            SELECT symbol, 
                   COUNT(*) as count,
                   SUM(volume) as total_volume,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price
            FROM trades
            GROUP BY symbol
        """)
        symbol_stats = cursor.fetchall()
        
        # Book snapshots
        cursor.execute("SELECT COUNT(*) as total FROM book_snapshots")
        book_count = cursor.fetchone()['total']
        
        # Níveis
        cursor.execute("SELECT COUNT(*) as total FROM price_levels WHERE strength >= 3")
        level_count = cursor.fetchone()['total']
        
        print(f"📊 {Colors.BOLD}Resumo Geral:{Colors.RESET}")
        print(f"   • Total de Trades: {trade_stats['total']:,}")
        print(f"   • Primeiro Trade: {trade_stats['first']}")
        print(f"   • Último Trade: {trade_stats['last']}")
        print(f"   • Book Snapshots: {book_count:,}")
        print(f"   • Níveis Significativos: {level_count}")
        
        print(f"\n📈 {Colors.BOLD}Por Símbolo:{Colors.RESET}")
        for stat in symbol_stats:
            print(f"\n   {Colors.BLUE}{stat['symbol']}{Colors.RESET}:")
            print(f"   • Trades: {stat['count']:,}")
            print(f"   • Volume Total: {stat['total_volume']:,}")
            print(f"   • Preço Médio: ${stat['avg_price']:.1f}")
            print(f"   • Range: ${stat['min_price']:.1f} - ${stat['max_price']:.1f}")
            
    def custom_query(self):
        """Permite executar queries customizadas"""
        print(f"\n{Colors.BOLD}=== QUERY CUSTOMIZADA ==={Colors.RESET}")
        print("Digite 'exit' para sair\n")
        
        while True:
            query = input("SQL> ").strip()
            
            if query.lower() == 'exit':
                break
                
            if not query:
                continue
                
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                
                if query.upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    if results:
                        # Pega nomes das colunas
                        columns = [desc[0] for desc in cursor.description]
                        
                        # Prepara dados
                        table_data = []
                        for row in results[:50]:  # Limita a 50 linhas
                            table_data.append(list(row))
                            
                        print(tabulate(table_data, headers=columns, tablefmt='grid'))
                        
                        if len(results) > 50:
                            print(f"\n... e mais {len(results) - 50} linhas")
                    else:
                        print("Nenhum resultado encontrado.")
                else:
                    self.conn.commit()
                    print(f"✅ Query executada com sucesso. Linhas afetadas: {cursor.rowcount}")
                    
            except Exception as e:
                print(f"{Colors.RED}Erro: {e}{Colors.RESET}")
                
def main():
    """Menu principal"""
    viewer = DatabaseViewer()
    
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print(f"{Colors.BOLD}=== VISUALIZADOR DE BANCO DE DADOS ==={Colors.RESET}")
        print("\n1. Ver últimos trades")
        print("2. Perfil de volume")
        print("3. Níveis de suporte/resistência")
        print("4. Book snapshots")
        print("5. Estatísticas gerais")
        print("6. Query customizada")
        print("0. Sair")
        
        choice = input("\nEscolha: ")
        
        if choice == '1':
            limit = input("Quantos trades? (padrão: 20): ") or "20"
            viewer.view_recent_trades(int(limit))
        elif choice == '2':
            symbol = input("Símbolo (WDOFUT/DOLFUT): ").upper() or "WDOFUT"
            viewer.view_volume_profile(symbol)
        elif choice == '3':
            symbol = input("Símbolo (WDOFUT/DOLFUT): ").upper() or "WDOFUT"
            viewer.view_support_resistance(symbol)
        elif choice == '4':
            symbol = input("Símbolo (WDOFUT/DOLFUT): ").upper() or "WDOFUT"
            viewer.view_book_snapshots(symbol)
        elif choice == '5':
            viewer.view_statistics()
        elif choice == '6':
            viewer.custom_query()
        elif choice == '0':
            break
            
        if choice != '0':
            input(f"\n{Colors.YELLOW}Pressione ENTER para continuar...{Colors.RESET}")
            
    viewer.conn.close()
    
if __name__ == "__main__":
    main()