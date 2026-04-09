"""
Scoring weights configuration for OB CASH.

These weights determine how much each condition contributes to the final score.
Total weight should sum to 1.0 for proper normalization.
"""

from dataclasses import dataclass
from typing import Dict

@dataclass
class IndicatorWeights:
    """Weights for each indicator/condition."""
    rsi_zone: float = 0.15
    macd_alignment: float = 0.15
    ema_alignment: float = 0.15
    stoch_zone: float = 0.10
    adx_strength: float = 0.10
    sma_slope: float = 0.05
    momentum: float = 0.05
    price_momentum: float = 0.05
    volume_spike: float = 0.10
    bb_width: float = 0.05
    bb_position: float = 0.05
    pattern_bonus: float = 0.00  # Disabled by default

    def as_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return {
            'rsi_zone': self.rsi_zone,
            'macd_alignment': self.macd_alignment,
            'ema_alignment': self.ema_alignment,
            'stoch_zone': self.stoch_zone,
            'adx_strength': self.adx_strength,
            'sma_slope': self.sma_slope,
            'momentum': self.momentum,
            'price_momentum': self.price_momentum,
            'volume_spike': self.volume_spike,
            'bb_width': self.bb_width,
            'bb_position': self.bb_position,
            'pattern_bonus': self.pattern_bonus
        }

# Default weights (conser宀
DEFAULT_WEIGHTS = IndicatorWeights()

# Aggressive weights (more signals, potentially lower quality)
AGGRESSIVE_WEIGHTS = IndicatorWeights(
    rsi_zone=0.10,
    macd_alignment=0.10,
    ema_alignment=0.10,
    stoch_zone=0.10,
    adx_strength=0.05,
    sma_slope=0.05,
    momentum=0.05,
    price_momentum=0.05,
    volume_spike=0.15,
    bb_width=0.05,
    bb_position=0.05,
    pattern_bonus=0.10
)

# Conservative weights (fewer but higher quality signals)
CONSERVATIVE_WEIGHTS = IndicatorWeights(
    rsi_zone=0.12,
    macd_alignment=0.13,
    ema_alignment=0.13,
    stoch_zone=0.10,
    adx_strength=0.15,
    sma_slope=0.03,
    momentum=0.03,
    price_momentum=0.03,
    volume_spike=0.10,
    bb_width=0.08,
    bb_position=0.10,
    pattern_bonus=0.00
)
