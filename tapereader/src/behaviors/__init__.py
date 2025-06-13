"""
Módulo de detecção de comportamentos de mercado
"""
from .absorption import AbsorptionDetector
from .exhaustion import ExhaustionDetector

__all__ = [
    'AbsorptionDetector',
    'ExhaustionDetector'
]