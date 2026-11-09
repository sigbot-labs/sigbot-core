ARG NODE_IMAGE=docker.io/library/node:22-alpine
ARG WEB_IMAGE=docker.io/nginxinc/nginx-unprivileged:1.27-alpine
FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY web/package.json web/package-lock.json ./
RUN npm ci --ignore-scripts && node node_modules/esbuild/install.js
COPY web/ ./
RUN npm run build
FROM ${WEB_IMAGE}
COPY web/deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
