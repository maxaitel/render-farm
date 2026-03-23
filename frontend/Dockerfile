FROM node:22-bookworm-slim

WORKDIR /app

ARG BACKEND_ORIGIN=http://api:8000
ENV BACKEND_ORIGIN=${BACKEND_ORIGIN}
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

COPY package.json /app/package.json
RUN npm install

COPY . /app

RUN npm run build
RUN mkdir -p /app/.next/standalone/.next \
  && cp -R /app/.next/static /app/.next/standalone/.next/static

EXPOSE 3000

CMD ["node", ".next/standalone/server.js"]
