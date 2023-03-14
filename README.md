# WordOfMouthETL
The purpose of this project is to populate database for the Word of Mouth project with game, genre, developer, publisher,
and platform info from Wikipedia.

To use this project you'll need Python installed and a file called settings.yml with the following properties
```
database:
  server: localhost
  port: 3306
  username: username
  password: password
wiki_api:
  client_id: client
  client_secret: secret
  access_token: token
```