# Build Stage
FROM node:16.3.0-alpine as builder
WORKDIR /usr/app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# stage 2
FROM node:16.3.0-alpine
WORKDIR /usr/app
COPY package*.json ./
RUN npm install

COPY --from=builder /usr/app/dist ./dist

EXPOSE 5000
CMD node dist/index.js