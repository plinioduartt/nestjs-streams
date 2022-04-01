FROM node:alpine

WORKDIR /usr/app

COPY package*.json ./
COPY dataset* ./

RUN npm install

COPY . .

CMD ["npm", "start"]