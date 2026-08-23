# Dockerfile
FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY index.js .
RUN mkdir -p /tmp/todus_uploads
EXPOSE 10000
CMD ["node", "index.js"]