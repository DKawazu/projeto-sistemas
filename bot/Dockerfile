FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY bot.js .
CMD ["node", "bot.js"]
