# Stage 1: Builder, official Node.js image to install dependencies and build the app.
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files first, Docker caches this layer.
# If only source files change (not package.json), npm install is skipped → faster builds.
COPY package.json package-lock.json ./
RUN npm ci --silent

# Copy the rest of the source code and build.
# VITE_API_BASE_URL tells the React app where to find the Go API at runtime.
COPY . .

# Accept the build argument from docker-compose.
# Vite reads VITE_* env vars at BUILD time and bakes them into the JS bundle.
ARG VITE_API_BASE_URL
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

RUN npm run build

# Stage 2: Runner 
# Uses the tiny NGINX image to serve the static files.
# Final image size: ~25MB (compared to ~400MB if we kept Node.js).
FROM nginx:stable-alpine

# Copy our custom NGINX config that handles React Router and API proxying.
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy the built React app from the builder stage.
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
