# One image for both the API and the workers; docker-compose picks the command.
FROM node:24-slim
WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Node 24 runs the TypeScript sources directly (type stripping), so there is no build step.
COPY src ./src
COPY migrations ./migrations

USER node
CMD ["node", "src/api.ts"]
