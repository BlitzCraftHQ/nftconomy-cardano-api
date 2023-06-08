# BlitzCraftHQ - API
-----


###### Use `yarn` to install the project dependencies:
```bash
yarn install
```

###### Type ```cp .env.example .env``` in the terminal to create an ENV file and enter your credentials in the .env file.
```sh
# Configure your API port
API_PORT=5000

# Auth0 Config
APP_BASE_URL=http://localhost:5000 
AUTH0_ISSUER_BASE_URL=https://yourtenant.eu.auth0.com/ 

# MongoDB Config
DATABASE_URL="DB_CONNECTION_STRING"
```
 
###### Run the project in development mode. Server restarts on file changes
```sh
yarn run dev
```

###### Deploy to production.
```sh
yarn run prod
```


