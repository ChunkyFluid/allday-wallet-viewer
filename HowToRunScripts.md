

###### **Regular data refresh (Run to update the live site)**

Refresh NFT metadata (players, teams, series, set, tier, etc.) - **node scripts/sync\_nft\_core\_metadata\_from\_snowflake.js**

Refresh wallet holdings (who owns what, locked/unlocked) - **node scripts/sync\_wallet\_holdings\_from\_snowflake.js**

Refresh wallet profiles (display names like ChunkyFluid) - **node scripts/sync\_wallet\_profiles\_from\_dapper.js**


###### **Refreshing the snapshot tables (for speed)**

Refresh top\_wallets\_snapshot - **node etl\_top\_wallets\_snapshot.js**

Refresh top\_holders\_snapshot (per-edition) - **node etl\_top\_holders\_snapshot.js**

Refresh wallet\_profile\_stats / explorer snapshots (if present) **- node etl\_wallet\_profile\_stats.js** and **node etl\_explorer\_filters\_snapshot.js**



###### **Heavy full refresh (good for once a week)**

cd C:\\Users\\kseaver\\OneDrive\\NFL\\allday-wallet-viewer



node scripts/sync\_nft\_core\_metadata\_from\_snowflake.js

node scripts/sync\_wallet\_holdings\_from\_snowflake.js

node scripts/sync\_wallet\_profiles\_from\_dapper.js

node scripts/load\_edition\_price\_stats\_from\_otm.js

node etl\_top\_wallets\_snapshot.js

node etl\_top\_holders\_snapshot.js

node etl\_wallet\_profile\_stats.js



###### **Quick refresh (good for daily / before you check your own wallet)**

cd C:\\Users\\kseaver\\OneDrive\\NFL\\allday-wallet-viewer



node scripts/sync\_wallet\_holdings\_from\_snowflake.js

node scripts/sync\_wallet\_profiles\_from\_dapper.js

node etl\_top\_wallets\_snapshot.js



