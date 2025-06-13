import sys
import os

# Adiciona diretorio tapereader ao path
tapereader_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, tapereader_path)
