SPLIT_QUERY = '''
Before executing the natural language query we have to identify the definition of the nodes and connections. For example:

----
Query: Garfield and his friends
Nodes: characters in the Garfield comic strip
Connected: if they are friends
----
Query: 19th century composers born in the same place
Nodes: 19th century composers
Connected: if they were born in the same city
----
Query: twin cities in Europe
Nodes: the cities of Europe
Connected: if they are twin cities
----
Query: English authors by genre
Nodes: English authors
Connected: if they wrote in the same genre
----
Query: defense industry companies by ownership
Nodes: defense industry companies
Connected: if one is a subsidiary of the other
----
Query: rivers of America and how they flow into each other
Nodes: the rivers in the USA
Connected: if one flows into the other
----
Query: QUERY
Nodes:
'''

ENTITY = '''
----
Which WikiData entities and properties are relevant if we want to find the 19th century composers and connect them if they were born in the same city?

Entities:
- human
- composer
- city

Properties:
- birth date
- occupation
- birthplace

----
Which WikiData entities and properties are relevant if we want to find the English authors and connect them if they wrote in the same genre?

Entities:
- human
- author
- United Kingdom

Properties:
- occupation
- country of citizenship
- genre

----
Which WikiData entities and properties are relevant if we want to find the cities of Europe and connect them if they are twin cities?

Entities:
- city
- Europe

Properties:
- twinned cities

----
Which WikiData entities and properties are relevant if we want to find the Disney characters and connect them if they appear in the same movie?

Entities:
- fictional character
- Walt Disney Pictures
- movie

Properties:
- production company
- distributed by
- present in work

----
Which WikiData entities and properties are relevant if we want to find the left-handed actresses and connect them if they played in the same movie?

Entities:
- human
- actor
- human female
- left-handedness

Properties:
- occupation
- gender
- handedness
- cast member

----
Which WikiData entities and properties are relevant if we want to find the NODES and connect them if EDGE_CONDITION?

Entities:
-'''

SPARQL = '''
####
# A SPARQL query for WikiData to find the 19th century composers
# and collect all the data needed to connect them if they were born in the same city.
# Relevant WikiData entities and properties for reference:
# - instance of: wdt:P31
# - subclass of: wdt:P279
# - human: wd:Q5
# - occupation: wdt:P106
# - composer: wd:Q36834
# - birth date: wdt:P569
# - birthplace: wdt:P19
# - city: wd:Q515
SELECT ?composer ?composerLabel ?birthDate ?birthPlaceLabel
WHERE
{
  ?composer wdt:P106 wd:Q36834 .
  ?composer wdt:P569 ?birthDate .
  FILTER(YEAR(?birthDate) >= 1800 && YEAR(?birthDate) < 1900) .
  ?composer wdt:P19 ?birthPlace .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

####
# A SPARQL query for WikiData to find Disney characters
# and collect all the data needed to connect them if they appear in the same movie.
# Relevant WikiData entities and properties for reference:
# - instance of: wdt:P31
# - subclass of: wdt:P279
# - human: wd:Q5
# - fictional character: wd:Q95074
# - Walt Disney Pictures: wd:Q191224
# - movie: wd:Q11424
# - distributed by: wdt:P750
# - production company: wdt:P272
# - present in work: wdt:P1441
SELECT ?character ?characterLabel ?movie ?movieLabel
WHERE
{
  ?character wdt:P31/wdt:P279* wd:Q95074 .
  ?character wdt:P1441 ?movie .
  ?movie wdt:P31 wd:Q11424 .
  ?movie wdt:P272 wd:Q191224 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

####
# A SPARQL query for WikiData to find the left-handed actresses
# and collect all the data needed to connect them if they appear in the same movie.
# Relevant WikiData entities and properties for reference:
# - instance of: wdt:P31
# - subclass of: wdt:P279
# - human: wd:Q5
# - actor: wd:Q33999
# - human female: wd:Q6581072
# - left-handedness: wd:Q789447
# - occupation: wdt:P106
# - gender: wdt:P21
# - handedness: wdt:P552
# - cast member: wdt:P161
SELECT ?actor ?actorLabel ?movie ?movieLabel
WHERE
{
  ?actor wdt:P21 wd:Q6581072 .
  ?actor wdt:P106 wd:Q33999 .
  ?actor wdt:P552 wd:Q789447 .
  ?movie wdt:P161 ?actor .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

####
# A SPARQL query for WikiData to find the NODES
# and collect all the data needed to connect them if EDGE_CONDITION.
# Relevant WikiData entities and properties for reference:
# - instance of: wdt:P31
# - subclass of: wdt:P279
# - human: wd:Q5
IDS
SELECT
'''

NODES_SQL = '''
CREATE TABLE wikidata_results (character text, characterLabel text, movie text, movieLabel text);
/*
    character   characterLabel      movie                                         movieLabel
0    Q2278783        Tia Dalma     Q54274           Pirates of the Caribbean: At World's End
1    Q2278783        Tia Dalma    Q161087         Pirates of the Caribbean: Dead Man's Chest
2    Q2635399             Iago  Q27188178                                            Aladdin
*/
-- Get Disney characters:
SELECT DISTINCT `character` as key, characterLabel FROM wikidata_results;

CREATE TABLE wikidata_results (birthDate text, birthPlaceLabel text, composer text, composerLabel text);
/*
                birthDate birthPlaceLabel   composer              composerLabel
0    1885-12-22T00:00:00Z   New York City    Q284386               Deems Taylor
1    1840-03-08T00:00:00Z          Verona    Q525052              Franco Faccio
2    1817-11-12T00:00:00Z          Verona    Q766389             Carlo Pedrotti
*/
-- Get 19th century composers:
SELECT DISTINCT composer as key, composerLabel FROM wikidata_results;

CREATE TABLE wikidata_results (actor text, actorLabel text, movie text, movieLabel text);
/*
       actor         actorLabel       movie                                         movieLabel
0    Q238305       Angie Harmon       Q2293                                              Chuck
1    Q238305       Angie Harmon       Q4456                                    Rizzoli & Isles
2    Q238305       Angie Harmon     Q132266                             Fun with Dick and Jane
*/
-- Get left-handed actresses:
SELECT DISTINCT actor as key, actorLabel FROM wikidata_results;

CREATE TABLE wikidata_results (SCHEMA);
/*
SAMPLE
*/
-- Get NODES:
SELECT
'''

EDGES_SQL = '''
CREATE TABLE wikidata_results (character text, characterLabel text, movie text, movieLabel text);
/*
    character   characterLabel      movie                                         movieLabel
0    Q2278783        Tia Dalma     Q54274           Pirates of the Caribbean: At World's End
1    Q2278783        Tia Dalma    Q161087         Pirates of the Caribbean: Dead Man's Chest
2    Q2635399             Iago  Q27188178                                            Aladdin
*/
-- Get all pairs of Disney characters if they appear in the same movie:
SELECT
 a.`character` as a_key, b.`character` as b_key, a.movie as movie, a.movieLabel as movieLabel
FROM wikidata_results a
INNER JOIN wikidata_results b
ON a.movie = b.movie;

CREATE TABLE wikidata_results (birthDate text, birthPlaceLabel text, composer text, composerLabel text);
/*
                birthDate birthPlaceLabel   composer              composerLabel
0    1885-12-22T00:00:00Z   New York City    Q284386               Deems Taylor
1    1840-03-08T00:00:00Z          Verona    Q525052              Franco Faccio
2    1817-11-12T00:00:00Z          Verona    Q766389             Carlo Pedrotti
*/
-- Get all pairs of 19th century composers if they were born in the same city:
SELECT
 a.composer as a_key, b.composer as b_key, a.birthPlaceLabel as birthPlaceLabel
FROM wikidata_results a
INNER JOIN wikidata_results b
ON a.birthPlaceLabel = b.birthPlaceLabel;

CREATE TABLE wikidata_results (actor text, actorLabel text, movie text, movieLabel text);
/*
       actor         actorLabel       movie                                         movieLabel
0    Q238305       Angie Harmon       Q2293                                              Chuck
1    Q238305       Angie Harmon       Q4456                                    Rizzoli & Isles
2    Q238305       Angie Harmon     Q132266                             Fun with Dick and Jane
*/
-- Get all pairs of left-handed actresses if they appear in the same movie:
SELECT
 a.actor as a_key, b.actor as b_key, a.movie as movie, a.movieLabel as movieLabel
FROM wikidata_results a
INNER JOIN wikidata_results b
ON a.movie = b.movie;

CREATE TABLE wikidata_results (SCHEMA);
/*
SAMPLE
*/
-- Get all pairs of NODES if EDGE_CONDITION:
SELECT
'''
