FROM node:22-bookworm-slim

WORKDIR /app

ARG BACKEND_ORIGIN=http://api:8000
ARG NEXT_PUBLIC_ADMIN_PANEL_PATH=control-tower
ENV BACKEND_ORIGIN=${BACKEND_ORIGIN}
ENV NEXT_PUBLIC_ADMIN_PANEL_PATH=${NEXT_PUBLIC_ADMIN_PANEL_PATH}
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

COPY package.json /app/package.json
RUN npm install

COPY . /app

RUN npm run build
RUN mkdir -p /app/.next/standalone/app/.next \
  && cp -R /app/.next/static /app/.next/standalone/app/.next/static

EXPOSE 3000

CMD ["node", ".next/standalone/app/server.js"]
