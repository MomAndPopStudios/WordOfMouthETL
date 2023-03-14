# WordOfMouthETL
The purpose of this project is to populate database for the Word of Mouth project with game, genre, developer, publisher,
and platform info from Wikipedia.

To use this project you'll need Python installed and a file called connections.json with the following properties
```
{
  "database_url": "mariadb+pymysql://username:password@127.0.0.1:3306/word_of_mouth"
}
```