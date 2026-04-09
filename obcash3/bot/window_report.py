from __future__ import absolute_import
"""
FREE-window summary rendering helpers.

Prepared so the report payload can later feed an image generator without
changing the scheduler contract.
"""

from dataclasses import dataclass
from typing import Any, Dict

from obcash3.bot.commercial import build_auto_vip_promo_message
from obcash3.bot.signal_store import WindowStats


@dataclass(frozen=True)
class WindowReportPayload:
    date: str
    window: str
    wins: int
    losses: int
    total_signals: int
    accuracy_percent: float
    estimated_profit: float
    branding: str = "MagnataZ"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "window": self.window,
            "wins": self.wins,
            "losses": self.losses,
            "total_signals": self.total_signals,
            "accuracy_percent": self.accuracy_percent,
            "estimated_profit": self.estimated_profit,
            "branding": self.branding,
        }


def should_send_positive_image(total_wins: int, total_losses: int) -> bool:
    """Positive result art is sent only when the window closes with at least one WIN."""
    del total_losses
    return int(total_wins) >= 1


def build_window_report_payload(stats: WindowStats) -> WindowReportPayload:
    """Future-proof report payload for text and, later, branded images."""
    return WindowReportPayload(
        date=stats.date,
        window=stats.window,
        wins=stats.total_wins,
        losses=stats.total_losses,
        total_signals=stats.total_signals,
        accuracy_percent=stats.assertividade_percentual,
        estimated_profit=stats.lucro_total_estimado,
    )


def build_window_feedback(window: str, total_wins: int, total_losses: int, estimated_profit: float = 0.0) -> str:
    """Render the final feedback text for each FREE window outcome."""
    lines = [f"📊 Resultado da janela das {window}", ""]

    if int(total_wins) >= 2 and int(total_losses) == 0:
        lines.extend(
            [
                "✅ 2 WIN em 2 entradas",
                "",
                "📈 Operações concluídas com sucesso.",
                "",
                "Seguimos firmes em busca das melhores oportunidades do dia.",
            ]
        )
    elif int(total_wins) == 1 and int(total_losses) == 1:
        lines.extend(
            [
                "✅ 1 WIN",
                "❌ 1 LOSS",
                "",
                "📈 Encerramos a janela com resultado positivo.",
                "",
                "Seguimos focados nas próximas oportunidades.",
            ]
        )
    else:
        lines.extend(
            [
                "❌ Janela encerrada sem resultado positivo.",
                "",
                "Seguimos com disciplina e foco nas próximas oportunidades.",
            ]
        )

    if abs(float(estimated_profit or 0.0)) > 0.009:
        lines.extend(["", f"💰 Resultado estimado da janela: R$ {float(estimated_profit):.2f}"])

    return "\n".join(lines)


def build_window_report_message(stats: WindowStats) -> str:
    """Backward-compatible wrapper used by the scheduler."""
    return build_window_feedback(
        window=stats.window,
        total_wins=stats.total_wins,
        total_losses=stats.total_losses,
        estimated_profit=stats.lucro_total_estimado,
    )


def build_window_cta_message(config: Any = None) -> str:
    """CTA sent once after each completed FREE window report."""
    return build_auto_vip_promo_message(config)
