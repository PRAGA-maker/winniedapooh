"""
NeuralLambda forecasting method - In-context weight updates via Low-Rank (LoR) modules.

Based on neurallambda sorting-experiment branch (external/neurallambda).
"""

from .data_format import example_to_neurallambda_format

__all__ = ['example_to_neurallambda_format']
