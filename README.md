# MagnataZ Bot

Repositório standalone do bot do Telegram do MagnataZ, preparado para GitHub e Render como `Background Worker`.

## Estrutura

- `bot.py`
- `requirements.txt`
- `README.md`
- `config.example.json`
- `obcash3/`
- `runtime/`

## Execução local

1. Crie um ambiente virtual.
2. Instale as dependências:

```powershell
pip install -r requirements.txt
```

3. Configure por variáveis de ambiente ou, se quiser testar localmente, copie:

```powershell
Copy-Item config.example.json config.json
```

4. Inicie o bot:

```powershell
python bot.py
```

## Variáveis de ambiente

Obrigatórias em produção:

```text
BOT_TOKEN=
FREE_GROUP_ID=
VIP_GROUP_ID=
KIWIFY_URL=
LOVABLE_URL=
```

Também suportadas:

```text
TWELVE_API_KEY=
ALPHA_VANTAGE_API_KEY=
TELEGRAM_CHAT_ID=
GROUP_TIER=free
MESSAGE_MODE=vip
PRIVATE_WELCOME_LINK=https://t.me/MagnataZ_Bot?start=welcome
FREE_GROUP_LINK=https://t.me/+2-sVI86sGzQ1MmEx
APP_RUNTIME_DIR=
CONFIG_PATH=
```

## GitHub

Suba a pasta inteira `bot_github`.

Arquivos sensíveis já ficam fora do commit por `.gitignore`:

- `config.json`
- `.env`
- `runtime/` gerado em produção
- logs, cache, históricos e modelos

## Render

Tipo de serviço:

```text
Background Worker
```

Build Command:

```text
pip install -r requirements.txt
```

Start Command:

```text
python bot.py
```

## Treino manual do ML

```powershell
python train_model.py --min-samples 24
```

## Persistência em runtime

O bot usa a pasta `runtime/` para:

- histórico de sinais
- estado do funil privado
- logs
- cache
- artefatos do ML

## Observações

- `config.json` não é obrigatório.
- Em produção, as variáveis de ambiente têm prioridade sobre arquivo local.
- O projeto não depende do `.exe` nem da interface gráfica para rodar no Render.
