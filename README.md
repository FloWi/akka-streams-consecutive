# Akka Streams Consecutive

Example of how to expand a stream into one with consecutive elements.

Usecase: Product catalogs of different tenants have to be merged together for displaying them localized.

Requirement: the streams have to be sorted (easily doable by querying them sorted from the underlying datastore)

## deProducts

|  id | name                        |
| --: | --------------------------- |
|   1 | Deutsches Produkt mit id 1  |
|   2 | Deutsches Produkt mit id 2  |
|   5 | Deutsches Produkt mit id 5  |
|   8 | Deutsches Produkt mit id 8  |
|   9 | Deutsches Produkt mit id 9  |
|  10 | Deutsches Produkt mit id 10 |
|  12 | Deutsches Produkt mit id 12 |

## enProducts

|  id | name                       |
| --: | -------------------------- |
|   2 | English Product with id 2  |
|   4 | English Product with id 4  |
|   8 | English Product with id 8  |
|   9 | English Product with id 9  |
|  11 | English Product with id 11 |

## zipped

| de id | de product                  | en id | en product                 |
| ----: | --------------------------- | ----- | -------------------------- |
|     1 | Deutsches Produkt mit id 1  | 1     | ---                        |
|     2 | Deutsches Produkt mit id 2  | 2     | English Product with id 2  |
|     3 | ---                         | 3     | ---                        |
|     4 | ---                         | 4     | English Product with id 4  |
|     5 | Deutsches Produkt mit id 5  | 5     | ---                        |
|     6 | ---                         | 6     | ---                        |
|     7 | ---                         | 7     | ---                        |
|     8 | Deutsches Produkt mit id 8  | 8     | English Product with id 8  |
|     9 | Deutsches Produkt mit id 9  | 9     | English Product with id 9  |
|    10 | Deutsches Produkt mit id 10 | 10    | ---                        |
|    11 | ---                         | 11    | English Product with id 11 |
|    12 | Deutsches Produkt mit id 12 | 12    | ---                        |

## cleaned up (empty elements removed)

| de id | de product                  | en id | en product                 |
| ----: | --------------------------- | ----- | -------------------------- |
|     1 | Deutsches Produkt mit id 1  | 1     | ---                        |
|     2 | Deutsches Produkt mit id 2  | 2     | English Product with id 2  |
|     4 | ---                         | 4     | English Product with id 4  |
|     5 | Deutsches Produkt mit id 5  | 5     | ---                        |
|     8 | Deutsches Produkt mit id 8  | 8     | English Product with id 8  |
|     9 | Deutsches Produkt mit id 9  | 9     | English Product with id 9  |
|    10 | Deutsches Produkt mit id 10 | 10    | ---                        |
|    11 | ---                         | 11    | English Product with id 11 |
|    12 | Deutsches Produkt mit id 12 | 12    | ---                        |
