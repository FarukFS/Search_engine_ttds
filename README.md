The way the search engine works is as follows: User enters a query, for example: "Black wall". After processing the query, the system will return all songs that contain the words Black and wall (can also be treated as a phrase rather than two separate words). The result shown is ranked according to different models. By default, the Okapi BM25 model is used; however, a TF-IDF model is also available.

Frontend created using React, backend using Python. Data collection stored in a MongoDB. API and database deployed using docker. Website deployed using Heroku (it is now down since the backend server is offline).

This search engine uses an inverted positional index for searching the queries entered by the user. It supports boolean and ranked search, as well as phrase and proximity queries.

The dataset is not avaliable in this repository, but it consists of a collection of song names + lyrics. For its creation, different datasets from Kaggle were combined. However, this was not enough, so a web scraping of Genius.com allowed the collection of more songs, together with their thumbails and genres.
