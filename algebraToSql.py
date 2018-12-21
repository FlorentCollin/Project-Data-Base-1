import copy
import sqlite3
import pandas as pd

class DBSchema(object):
    """Classe représentant un schema de base de données."""
    def __init__(self, tables = None):
        if tables != None:
            if not isinstance(tables, dict):
                raise TypeError(messageError(tables, "tables", "dict"))
            if not all(isinstance(name, str) for name in list(tables.keys())):
                raise TypeError("Keys of tables must be string")
            if not all(isinstance(table, Table) for table in list(tables.values())):
                raise TypeError("Values of tables must be Table")
            self.tables = tables
        else:
            self.tables = {}

    def addTable(self, table):
        if not isinstance(table, Table):
            raise TypeError(table + " : table must be a Table")
        if not hasattr(self, "tables"):
            self.tables = {table.name:table}
        else:
            self.tables[table.name] = table

    def get(self, name):
        return self.tables[name]

    def __str__(self):
        res = ""
        for table in list(self.tables.values()):
            res += table.__str__()+"\n"
        return res

    def __repr__(self):
        return __str__


class Table(object):
    """Classe permettant de stocker une table
        Args:
            name (str): nom de la table
            schema (dict of (String:String): attributs de la table (autrement dit, les colonnes de la table)
                        ex : schema = {"Name":"TEXT", "Number:"INTEGER"}"""
    def __init__(self,name, schema):
        #Vérification des types
        if not isinstance(name, str):
            raise TypeError(name + " : name must be a String")

        if not isinstance(schema, dict):
            raise TypeError(errorMessage(schema, "schema", "Dict"))

        if not all(isinstance(key, str) for key in list(schema.keys())):
            raise TypeError("Keys of schema must be String")

        if not all(isinstance(value, str) for value in list(schema.values())):
            raise TypeError("Values of schema must be String")
        self.name = name
        self.schema = schema

    """Méthode permettant de savoir si un attribut appartient à la relation
        Args:
            attribute (str): attribut à tester
            relationName (str) : nom de la relation
        Returns:
            bool: True si l'attribut est un attribut de la relation, faux sinon"""
    def isAttribute(self, attribute):
        if attribute in list(self.schema.keys()):
            return True
        return False

    """
        Méthode permettant de connaître le type d'un attribut
        Args :
            attribute (str) : attribut dont on veut connaître le type
            relationName (str) : nom de la ration dont doit appartenir l'attribut
        Returns :
            Le type de l'attribut si l'attribut appartient à la relation
    """
    def getAttributeType(self, attribute):
        if not self.isAttribute(attribute):
            raise AttributeError(str(attribute) + " is not an attribute of the table")
        return self.schema[attribute]

    """Méthode qui vérifie si le type de value correspond au type de l'attribut
        Args:
            attribute (str): la colonne correspondant à la value
            value : valeur dont on veut vérifier si le type correspond bien à la colonne (attribut)
        Returns:
            bool: True si le type correspond, False sinon"""
    def checkType(self, attribute, value):
        attributeType = self.getAttributeType(attribute)
        #Remplacement du type python par le type accepté par SQLite3
        if isinstance(value, int):
            return attributeType == "INTEGER"
        elif isinstance(value, str):
            return attributeType == "TEXT"
        elif isinstance(value, float):
            return attributeType == "REAL"
        else:
            return False

    def __str__(self):
        names = "Name  |"
        types = "Types |"
        for attribute in self.schema:
            maximum = max(len(attribute), len(self.schema[attribute]))
            names += " "+attribute+" "*(maximum-len(attribute))+" |"
            types += " "+self.schema[attribute]+" "*(maximum-len(self.schema[attribute]))+" |"
        l = len(names)
        lenName = len(self.name)
        title = " "*(int(l/2)-int(lenName/2))+str(self.name)
        borders = "-"*l
        return title+"\n"+borders+"\n"+names+"\n"+types+"\n"+borders

    def __repr__(self):
        return __str__()

class Rel:
    """Classe représentant une relation. C'est la classe mère des classes des expressions algébriques SPJRUD
        Args:
            table (table or Rel): schéma de base de données ou relation sur lequel la nouvelle relation va se baser
            table2 (table or Rel): schéma de base de données ou relation utilisé par les classes Join, Union, Diff
                                         qui travaillent sur plusieurs tables ou Relations simultanément (optionnel)"""
    def __init__(self, table1, table2 = None):
        #Vérification des types + copie du schéma de base de données pour ne pas modifier le schéma d'origine
        if isinstance(table1, Table):
            """Vrai copie de table car certaines relations peuvent changer ce schema
                   et on ne veut pas avoir d'effet de bord"""
            self.table = copy.deepcopy(table1)

        elif isinstance(table1, Rel):
            self.table = copy.deepcopy(table1.table)

        else:
            raise ValueError(errorMessage(table2, "table2", "Table or Rel"))

        #Utilisé par la jointure, l'union et la différence qui travaillent sur plusieurs relations simultanément
        if not table2 == None:
            if isinstance(table2, Table):
                self.table2 = copy.deepcopy(table2)

            elif isinstance(table2, Rel):
                self.table2 = copy.deepcopy(table2.table)

            else:
                raise ValueError(errorMessage(table2, "table2", "Table or Rel"))

    """Méthode qui renvoie un String correspondant à la requête SQL de la relation."""
    def toSql(self, delimiters=False):
        return self.table.name

    def __str__(self):
        return "Rel(" + self.toSql() + ")"
    def __repr__(self):
        return self.__str__()

class Select(Rel):
    """Classe représentant la Sélection dans l'algèbre relationnelle (SPJRUD)
        Args:
            operation (Operation): operation à effectuer (ex: l'égalité d'un attribut et d'une constante (voir sous-classes d'Operation))
            rel (Rel): relation sur laquelle effectuer la sélection"""
    def __init__(self, operation, rel):
        super().__init__(rel)
        self.rel = rel


        if not isinstance(operation, Operation):
            raise ValueError(errorMessage(operation, "operation", "Operation"))
        self.operation = operation

        #On vérifie que l'attribut de l'opération est bien un attribut du table
        if not self.table.isAttribute(operation.param1):
            raise AttributeError('"' + operation.param1 + '"' + " in operation is not an attribute of the table")

        #Si le membre de droite de l'opération est aussi un attribut, on vérifie qu'il fait bien partie de la table
        if isinstance(operation.param2, Attribute) and not self.table.isAttribute(operation.param2.attribute):
            raise AttributeError('"' + operation.param2 + '"' + " in operation is not an attribute of the table")

        #Si le membre de droite de l'opération est une constante, on vérifie que le type de cette constante correspond au type de l'attribut
        if isinstance(operation.param2, Const) and not self.table.checkType(operation.param1, operation.param2.const):
            raise ValueError("The type of operation.param2 : " + str(operation.param2) + " doesn't correspond to attribute's type : " + str(self.table.getAttributeType(operation.param1)))

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel.toSql(True) + " where " + self.operation.toSql()

    def __str__(self):
        return "Select(" + str(self.operation) + ", " + str(self.rel) + ")"
    def __repr__(self):
        return self.__str__()

class Proj(Rel):
    """Classe représentant la Projection dans l'algèbre relationnelle (SPJRUD)
        Args:
            schema (list of string): liste d'attributs qu'il faut garder lors de la projection
            rel (Rel): relation sur laquelle effectuer la projection"""
    def __init__(self, attributes, rel):
        super().__init__(rel)
        self.rel = rel
        if not isinstance(attributes, list):
            raise ValueError(errorMessage(schema, "schema", "list"))

        #On vérifie que chaque attribut de la liste d'attributs donnée en argument est bien un attribut de la table
        for attribute in attributes:
            if not self.rel.table.isAttribute(attribute):
                raise ValueError(attribute + " is not an attribute in the table :" + self.table.name)

        #Mise à jour de la liste d'attributs pour ne garder que les attributs de liste donnée en argument
        self.table.schema = {key : self.table.schema[key] for key in attributes}
        self.attributes = attributes


    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select distinct "
        for attribute in self.attributes:
            res += attribute + ", "
        return res[:-2] + " from " + self.rel.toSql(True)

    def __str__(self):
        return "Proj(" + str(self.attributes) + ", " + str(self.rel) + ")"
    def __repr__(self):
        return self.__str__()


class Join(Rel):
    """Classe représentant la Jointure dans l'algèbre relationnelle (SPJRUD)
        Args:
            rel1 (Rel): relation de gauche de la jointure (rel1 x rel2)
            rel2 (Rel): relation de droite de la jointure (rel1 x rel2)"""
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)
        #On rajoute au dictionnaire d'attributs les attributs de rel2
        self.table.schema.update(self.table2.schema) #Vérifier que les colonnes ayant le m nom doivent avoir le m type
        self.rel1 = rel1
        self.rel2 = rel2

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " natural join " + "(select * from " + self.rel2.toSql(True) + ")"

    def __str__(self):
        return "Join(" + str(self.rel1) + ", " + str(self.rel2) + ")"
    def __repr__(self):
        return self.__str__()

class Rename(Rel):
    """Classe représentant le Rennomage dans l'algèbre relationnelle (SPJRUD)
        Args:
            attributeFrom (str): attribut à renommer
            attributeTo (str): String représentant le nouveau nom d'attributeFrom.
            rel (Rel): relation sur laquelle effectuer le renommage """
    def __init__(self, attributeFrom, attributeTo, rel):
        super().__init__(rel)
        self.rel = rel
        #On vérifie qu'attributeFrom est bien le nom d'un attribut du table
        if not self.table.isAttribute(attributeFrom):
            raise AttributeError(attributeFrom + " is not a name of an attribute")
        self.attributeFrom = attributeFrom

        if not isinstance(attributeTo, str):
            raise ValueError(errorMessage(attributeTo, "attributeTo", "String"))

        #On vérifie qu'attributeTo n'est pas déjà un nom d'attribut du table
        if self.table.isAttribute(attributeTo):
            raise AttributeError(attributeTo + " is already a name of another attribute")
        self.attributeTo = attributeTo

        #Ajout de l'attribut attributeTo au table
        self.table.schema[attributeTo] = self.table.schema[attributeFrom]
        #Supression de l'attribut attributeFrom au table
        del self.table.schema[attributeFrom]

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select "
        for i in list(self.rel.table.schema.keys()):
            if i == self.attributeFrom:
                res += i + ' "' + self.attributeTo + '", '
            else:
                res += i + ", "
        return res[:-2] + " from " + self.rel.toSql(True)



class Union(Rel):
    """Classe représentant l'Union dans l'algèbre relationnelle (SPJRUD)
        Args:
            rel1 (Rel): relation de gauche de l'union (rel1 u rel2)
            rel1 (Rel): relation de droite de l'union (rel1 u rel2) """
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)
        #On vérifie que les attributs de rel1 sont égaux au attributs de rel2 (ie: sorte(rel1) = sorte(rel2))
        if not set(self.table.schema.keys()) == set(self.table2.schema.keys()):
            raise SorteError("Sorte of rel1  must be equal to sorte of rel2")
        for attribute in list(self.table.schema.keys()):
            if not (self.table.getAttributeType(attribute) == self.table2.getAttributeType(attribute)):
                raise TypeError("All attribute's type of rel1 and rel2 must be equal")
        self.rel1 = rel1
        self.rel2 = rel2

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " union " + "select * from " + self.rel2.toSql(True)

    def __str__(self):
        return "Union(" + str(self.rel1) + ", " + str(self.rel2) + ")"
    def __repr__(self):
        return self.__str__()

class Diff(Union): #Hérite de Union car la différence à les mêmes restrictions sur les attributs des relations que l'union
    """Classe représentant la Différence dans l'algèbre relationnelle (SPJRUD)
        Args:
            rel1 (Rel): relation de gauche de la différence (rel1 - rel2)
            rel1 (Rel): relation de droite de la différence (rel1 - rel2) """
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " except " + "select * from " + self.rel2.toSql(True)

    def __str__(self):
        return "Diff(" + str(self.rel1) + ", " + str(self.rel2) + ")"
    def __repr__(self):
        return self.__str__()

class Operation:
    """Classe représentant une opération parmi (=, >, >=, <, <=)
        Args:
            symbol (str): symbole de l'opération
            param1 (str) : membre de gauche de l'opération, param1 doit être le nom d'un attribut
            param2 (Const ou Attribute): membre de droite de l'opération, param2 doit être une constante ou un nom d'attribut"""
    def __init__(self, symbol, param1, param2):
        if symbol != "=" and symbol != ">" and symbol != ">=" and symbol != "<" and symbol != "<=": #
            raise ValueError(errorMessage(symbol, "symbol", "=, >, >=, <, <="))
        self.symbol = symbol

        if not isinstance(param1, str):
            raise ValueError(errorMessage(param1, "param1", "String"))
        self.param1 = param1

        if not (isinstance(param2, Const) or isinstance(param2, Attribute)):
            raise ValueError(errorMessage(param2, "param2", "Const or Attribute"))
        self.param2 = param2

    def toSql(self):
        return str(self.param1) + self.symbol + self.param2.toSql()

class Eq(Operation):
    """Classe représentant l'équalité (=)"""
    def __init__(self, param1, param2):
        super().__init__("=", param1, param2)

    def __str__(self):
        return "Eq(" + str(self.param1) + ", " + str(self.param2) + ")"

    def __repr__(self):
        return self.__str__()

class Greather(Operation):
    """Classe représentant l'opération (>)"""
    def __init__(self, param1, param2):
        super().__init__(">", param1, param2)

    def __str__(self):
        return "Greather(" + str(self.param1) + ", " + str(self.param2) + ")"

    def __repr__(self):
        return self.__str__()

class GreatherOrEqual(Operation):
    """Classe représentant l'opération (>=)"""
    def __init__(self, param1, param2):
        super().__init__(">=", param1, param2)

    def __str__(self):
        return "GreatherOrEqual(" + str(self.param1) + ", " + str(self.param2) + ")"

    def __repr__(self):
        return self.__str__()

class Less(Operation):
    """Classe représentant l'opération (<)"""
    def __init__(self, param1, param2):
        super().__init__("<", param1, param2)

    def __str__(self):
        return "Less(" + str(self.param1) + ", " + str(self.param2) + ")"

    def __repr__(self):
        return self.__str__()

class LessOrEqual(Operation):
    """Classe représentant l'opération (<=)"""
    def __init__(self, param1, param2):
        super().__init__("<=", param1, param2)

    def __str__(self):
        return "LessOrEqual(" + str(self.param1) + ", " + str(self.param2) + ")"

    def __repr__(self):
        return self.__str__()



class Const:
    """Classe qui contient une constante, utilisé lors de la sélection"""
    def __init__(self, const):
        self.const = const

    def toSql(self):
        #On rajoute des "" si la constante est un String pour que la représentation en SQL soit correcte
        if isinstance(self.const, str):
            return '"' + self.const + '"'
        else:
            return str(self.const)

    def __str__(self):
        return "Const(" + str(self.const) + ")"

    def __repr__(self):
        return self.__str__()

class Attribute:
    """Classe qui contient un nom d'attribut, utilisé lors de la sélection"""
    def __init__(self, attribute):
        self.attribute = attribute

    def toSql(self):
        return self.attribute

    def __str__(self):
        return "Attribute(" + str(self.const) + ")"

    def __repr__(self):
        return self.__str__()
class AttributeError(Exception):
    def __init__(self, message):
        self.message = message

class SorteError(Exception):
    pass


class SQLite:
    def __init__(self, dbFile):
        if not isinstance(dbFile, str):
            raise TypeError(errorMessage(dbFile, "dbFile", "string"))
        print("extention : ",dbFile[-3:])
        if dbFile[-3:] != ".db":
            #soit on affiche juste un message stipulant qu'il n'y ait pas de table dans le fichier
            print("There is no data base in these file")
            #soit on lève carrément une erreur
            raise(TypeError("These file is not a data base file"))
        self.dbFile = dbFile
        self.dbSchema = self.getDBSchema()

    """
        Méthode permettant de récupérer le table d'une base de données
        Returns :
            Le table de la base de données
    """
    def getDBSchema(self):
        connexion = sqlite3.connect(self.dbFile)
        cursor = connexion.cursor()
        cursor.execute("select name from sqlite_master where type='table'")
        db = DBSchema()
        for row in cursor.fetchall():
            info = cursor.execute("select sql from sqlite_master where type='table' and name = '"+row[0]+"'").fetchone()
            info = info[0]
            parentPassed = False
            res = ""
            for i in info:
                if i == "(":
                    parentPassed = True
                if parentPassed == True:
                    res += i
            res = res[1:-1]
            res = res.split(",")
            schema = {}
            for sub in res:
                t = sub.split(" ")
                if(t[0] != ""):
                    schema[t[0]] = t[1]
                else:
                    schema[t[1]] = t[2]

            db.addTable(Table(row[0], schema))
        connexion.close()
        return db

    def execute(self, request, _print = False):
        if not isinstance(request, Rel):
            raise TypeError(errorMessage(request, "request", "Rel"))

        connexion = sqlite3.connect(self.dbFile)
        cursor = connexion.cursor()
        cursor.execute(request.toSql())
        result = cursor.fetchall()
        if _print and len(result) != 0:
            df = pd.DataFrame(result)
            df.columns = request.table.schema.keys()
            print(df)
        return result


def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + "."


if __name__ == "__main__":
    table = Table("emp", {"name":"TEXT", "sal":"INTEGER"})
    print(Diff(Rel(table),Rel(table)))
