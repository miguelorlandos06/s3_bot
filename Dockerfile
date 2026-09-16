FROM node:20-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/tg-api
RUN curl -L -o tg-api.tar.gz \
    https://github.com/tdlib/telegram-bot-api/releases/download/v10.3/telegram-bot-api-linux-x64.tar.gz \
    && tar -xzf tg-api.tar.gz \
    && rm tg-api.tar.gz \
    && chmod +x telegram-bot-api

WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .

RUN printf '#!/bin/sh\n\
mkdir -p /data/logs /data/temp\n\
/opt/tg-api/telegram-bot-api \\\n\
  --api-id=32471788 \\\n\
  --api-hash=cb57130abda56877acf3b3027e569450 \\\n\
  --http-port=8081 \\\n\
  --dir=/data \\\n\
  --temp-dir=/tmp \\\n\
  --local \\\n\
  --log=/data/logs/telegram-bot-api.log &\n\
sleep 3\n\
exec node /app/index.js\n' > /start.sh && chmod +x /start.sh

CMD ["/start.sh"]