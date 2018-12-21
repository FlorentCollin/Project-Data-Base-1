# Introduction
Ce rapport regroupe le guide d’utilisation de la librairie et les choix de modélisation. Le guide d’utilisation explique
en détail comment utiliser la librairie. Quant aux choix de modélisation ils reprennent l’explication des choix effectués.

# Guide-Utilisateur

Cette section a pour but d'expliquer et de montrer le fonctionnement de la librairie nommée *algebraToSql*.

### Création de requêtes SPJRUD
Afin de créer des requêtes en algèbre SPJRUD, il est nécessaire d'importer la librairie *algebraToSql*.

```python
from algebraToSql import *
```
Chaque requête nécessite de travailler sur une table. La class *Table* permet de créer une table, elle prend en paramètre le nom de cette table (String) et son schéma (dict).
Le schéma d'une table doit être un dictionnaire ayant comme clés des chaînes de caractères correspondant aux noms des attributs et comme valeurs, le type de l'attribut. Le type d'un attribut doit être parmi les types suivants :
* *TEXT*
* *INTEGER*
* *REAL*

```python
table = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
```
Il est aussi possible de créer un schéma de base de données qui contient des tables.

```python
employee = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
departement = Table("departement", {"name":"TEXT", "loc":"TEXT"})
db = DBSchema()
db.addTable(employee)
db.addTable(departement)
#La méthode get(name) permet de récupérer une table grâce à son nom.
employee = db.get("employee")
```
##### La classe Rel
La classe *Rel* est la classe mère, elle représente une relation.

```python
table = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
rel = Rel(table) #représente la relation associé à la table "employee"
```
