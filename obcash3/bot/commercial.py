from __future__ import absolute_import
"""
Commercial flow templates for the MagnataZ Telegram bot.
"""

import os
from typing import Any

FREE_GROUP_LINK = "https://t.me/+2-sVI86sGzQ1MmEx"
DEFAULT_PAYMENT_LINK = "https://pay.kiwify.com.br/Aty9Q61"
DEFAULT_LOVABLE_URL = "https://magnataz-vip-luxe.lovable.app"
DEFAULT_VIP_GROUP_LINK = "https://t.me/+Olel0chcCJgwMTZh"
DEFAULT_PRIVATE_WELCOME_LINK = "https://t.me/MagnataZ_Bot?start=welcome"
PRIVATE_WELCOME_BUTTON_TEXT = "Receber mensagem no privado"
AUTO_VIP_PROMO_EVERY_SIGNALS = 2
FREE_SIGNAL_WINDOWS = (
    ("window_08_09", "08:00", "09:00"),
    ("window_11_12", "11:00", "12:00"),
    ("window_18_19", "18:00", "19:00"),
)


def _config_value(config: Any, name: str, default: str = "") -> str:
    if config is not None:
        value = getattr(config, name, default)
        value = str(value or "").strip()
        if value:
            return value

    env_map = {
        "free_group_link": ("FREE_GROUP_LINK",),
        "private_welcome_link": ("PRIVATE_WELCOME_LINK",),
        "vip_payment_link": ("KIWIFY_URL", "VIP_PAYMENT_LINK", "PAYMENT_LINK"),
        "payment_link": ("KIWIFY_URL", "PAYMENT_LINK", "VIP_PAYMENT_LINK"),
        "lovable_url": ("LOVABLE_URL",),
        "vip_group_link": ("VIP_GROUP_LINK",),
    }
    for env_name in env_map.get(name, ()):
        env_value = os.getenv(env_name, "").strip()
        if env_value:
            return env_value

    return str(default or "").strip()


def free_group_link(config: Any = None) -> str:
    return _config_value(config, "free_group_link", FREE_GROUP_LINK)


def private_welcome_link(config: Any = None) -> str:
    return _config_value(config, "private_welcome_link", DEFAULT_PRIVATE_WELCOME_LINK)


def payment_link(config: Any = None) -> str:
    return _config_value(config, "payment_link", _config_value(config, "vip_payment_link", DEFAULT_PAYMENT_LINK))


def lovable_url(config: Any = None) -> str:
    return _config_value(config, "lovable_url", DEFAULT_LOVABLE_URL)


def vip_group_link(config: Any = None) -> str:
    return _config_value(config, "vip_group_link", DEFAULT_VIP_GROUP_LINK)


def build_free_welcome_message(config: Any = None) -> str:
    return (
        "Bem-vindo ao MagnataZ Free\n\n"
        "Aqui você recebe sinais gratuitos em horários estratégicos, com foco em oportunidades rápidas e objetivas na IQ Option.\n\n"
        "⏰ Horários\n"
        "• 08:00 às 09:00\n"
        "• 11:00 às 12:00\n"
        "• 18:00 às 19:00\n\n"
        "Operações\n"
        "As entradas são enviadas com ativo, horário e direção definidos, sempre com total clareza.\n\n"
        "Transparência\n"
        "Nossa equipe está à disposição para esclarecer qualquer dúvida.\n\n"
        "Para receber sinais durante todo o dia, acesse: /vip"
    )


def build_group_free_join_message(first_name: str) -> str:
    return (
        f"👋 Bem-vindo(a), {first_name}, ao MAGNATAZ FREE!\n\n"
        "Aqui você recebe entradas gratuitas em horários estratégicos para conhecer a força da nossa comunidade.\n\n"
        "💬 Quer receber uma mensagem especial no privado e conhecer o acesso VIP?\n\n"
        "Clique no botão abaixo 👇"
    )


def build_private_welcome_vip_message(config: Any = None) -> str:
    return (
        "🚀 Seja muito bem-vindo(a) ao universo MagnataZ!\n\n"
        "Você acabou de entrar no nosso grupo FREE, onde liberamos algumas oportunidades para você sentir o nível da nossa comunidade.\n\n"
        "Mas no VIP o jogo muda.\n\n"
        "Lá dentro você recebe acompanhamento muito mais completo, com entradas ao longo de TODO o dia, análises filtradas e acesso a benefícios exclusivos para operar com mais confiança.\n\n"
        "🎯 No VIP você encontra:\n"
        "• sinais durante TODO o dia\n"
        "• mais frequência de entradas\n"
        "• análises exclusivas da equipe\n"
        "• lives com trades ao vivo\n"
        "• sorteios de banca\n"
        "• comunidade ativa MagnataZ\n"
        "• acesso antecipado às novidades\n\n"
        "🌐 Conheça a página oficial:\n"
        f"{lovable_url(config)}\n\n"
        "⚡ Entre agora:\n"
        f"{payment_link(config)}\n\n"
        "✅ Após o pagamento, seu acesso ao grupo VIP será liberado."
    )


def build_vip_offer_message(config: Any = None) -> str:
    return (
        "🚀 MAGNATAZ VIP 🚀\n"
        "🔥 Acesso exclusivo para quem quer ir para o próximo nível\n\n"
        "Aqui você recebe sinais ao longo de TODO o dia, com entradas filtradas e acompanhadas para buscar as melhores oportunidades do mercado.\n\n"
        "🎯 O que você recebe no VIP\n"
        "• sinais durante TODO o dia\n"
        "• entradas com maior frequência\n"
        "• análise da nossa equipe especializada com apoio da IA exclusiva\n"
        "• sorteios de banca semanalmente\n"
        "• lives com trades ao vivo\n"
        "• comunidade ativa de membros VIP MagnataZ\n"
        "• acesso antecipado às novidades, atualizações e oportunidades exclusivas da família MagnataZ\n\n"
        "📌 Plano em destaque\n"
        "⭐ Vitalício promocional — R$ 49,90\n\n"
        "💎 Oferta promocional\n"
        "Por uma pequena diferença, você garante o acesso vitalício e recebe todas as atualizações futuras sem pagar mensalidade.\n\n"
        "🤝 Junte-se à família MagnataZ e opere com quem leva o mercado a sério.\n\n"
        "👀 Quer conhecer todos os benefícios, diferenciais e depoimentos da nossa comunidade?\n\n"
        "🌐 Conheça nossa página oficial de vendas:\n"
        f"{lovable_url(config)}\n\n"
        "⚡ Depois, garanta seu acesso imediato:\n"
        f"{payment_link(config)}\n\n"
        "✅ Após a confirmação do pagamento, seu acesso ao grupo VIP será liberado."
    )


def build_auto_vip_promo_message(config: Any = None) -> str:
    return (
        "Gostou das entradas free?\n\n"
        "No VIP você recebe sinais durante TODO o dia, com mais frequência, análises exclusivas e lives ao vivo.\n\n"
        "Veja todos os benefícios:\n"
        f"{lovable_url(config)}\n\n"
        "Entre agora:\n"
        f"{payment_link(config)}"
    )


def build_post_payment_dm_message(config: Any = None) -> str:
    return (
        "✅ Pagamento confirmado!\n\n"
        "Seja muito bem-vindo à família MagnataZ VIP.\n\n"
        "Seu acesso exclusivo já está liberado:\n\n"
        "🔒 Grupo Premium\n"
        f"{vip_group_link(config)}\n\n"
        "Desejamos ótimas operações!"
    )
