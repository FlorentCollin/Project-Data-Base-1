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

##### La classe Select
La classe *Select* représente la sélection dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre une opération (=,>,>=, <, <=) et une relation.

```python
table = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
s = Select(Eq("name", Const("Jean")), Rel(table))#Const représente une constanste
s = Select(Eq("number", Attribute("salary")), Rel(table))
#Attribute représente un attribut d'une table
s = Select(Greather("salary", Const(1500)), Rel(table))
s = Select(GreatherOrEqual("salary", Const(1000)), Rel(table))
s = Select(Less("salary", Const(2000)), Rel(table))
s = Select(LessOrEqal("salary", Const(1500)), Rel(table))
```

##### La classe Proj
La classe *Proj* représente la projection dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre une liste des attributs sur lesquels projeter et une relation.

```python
table = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
p = Proj(["name", "salary"], Rel(table))
```

##### La classe Join
La classe *Join* représente la jointure dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre deux relations.

```python
employee = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
departement = Table("departement", {"name":"TEXT", "loc":"TEXT"})
j = Join(Rel(employee), Rel(departement))
```

##### La classe Rename
La classe *Rename* représente le renommage dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre l'attribut à renommer (String), le nouveau nom de l'attribut (String) et une relation.

```python
table = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
r = Rename("name", "Nom", Rel(table))
```

##### La classe Union
La classe *Union* représente l'union dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre deux relations ayant les mêmes attributs

```python
employee = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
employee2 = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
j = Join(Rel(employee), Rel(employee2))
```

##### La classe Diff
La classe *Diff* représente la différence dans l'algèbre SPJRUD. Le constructeur de cette classe prend en paramètre deux relations ayant les mêmes attributs.

```python
employee = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
employee2 = Table("employee", {"name":"TEXT", "number":"INTEGER", "salary":"REAL"})
j = Diff(Rel(employee), Rel(employee2))
```
