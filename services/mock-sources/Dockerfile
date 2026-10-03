FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY src ./src
USER node
EXPOSE 4010
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD wget -qO- http://localhost:4010/health || exit 1
CMD ["node", "src/server.js"]
