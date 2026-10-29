FROM node:22-alpine AS build
WORKDIR /app
COPY src/web/package.json src/web/package-lock.json ./
RUN npm ci --ignore-scripts && node node_modules/esbuild/install.js
COPY src/web/ ./
RUN npm run build
FROM nginxinc/nginx-unprivileged:1.28-alpine
COPY src/web/deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
