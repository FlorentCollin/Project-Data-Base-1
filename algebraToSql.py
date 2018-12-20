import copy
import sqlite3


class DBSchema(object):
    """Classe permettant de stocker le schéma d'une base de données contenant une seule relation
        Args:
            name (str): nom du schéma de la base de données
            attributes (dict of (String:String): attributs de la relation (autrement dit, les colonnes de la relation)
                        ex : attributes = {"Name":"TEXT", "Number:"INTEGER"}"""
    def __init__(self, schemas = None):
        #Vérification des types
        if schemas != None:
            if not isinstance(schemas, dict):
                raise TypeError(errorMessage(schemas, "schemas", "Dict"))

            if not all(isinstance(key, str) for key in list(schemas.keys())):
                raise TypeError("Keys of schemas must be String")

            if not all(isinstance(value, dict) for value in list(schemas.values())):
                raise TypeError("Values of schemas must be dict")

            for subSchema in schemas.values():
                if not all(isinstance (key, str) for key in list(subSchema.keys())):
                    raise TypeError("Keys of subSchemas must be String")

                if not all(isinstance (value, str) for value in list(subSchema.values())):
                    raise TypeError("Values of subSchemas must be String")
            self.schemas = schemas
        else:
            self.schemas = {}

    def setSchema(self, schemas):
        self.schemas = schemas

    def hasSchemas(self):
        return self.schemas != {}

    def addRelation(self, name, relation):
        if not isinstance(name, str):
            raise TypeError(errorMessage(name, "name", "string"))
        
        if not isinstance(relation, dict):
            raise TypeError(errorMessage(relation, "relation", "dict"))

        if not all(isinstance(key, str) for key in list(relation.keys())):
            raise TypeError("Keys of relation must be String")
        
        if not all(isinstance(value, str) for value in list(relation.values())):
            raise TypeError("Values of relaiton must be String")

        self.schemas[name] = relation
        

    """Méthode permettant de savoir si un attribut appartient à la relation
        Args:
            attribute (str): attribut à tester
            relationName (str) : nom de la relation
        Returns:
            bool: True si l'attribut est un attribut de la relation, faux sinon"""
    def isAttribute(self, attribute, relationName):
        if relationName in self.schemas and attribute in self.schemas[relationName]:
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
    def getAttributeType(self, attribute, relationName):
        #Test de si la relation appartient à la base de donnée
        if self.isAttribute(attribute, relationName):
            for i in range(len(self.schemas.keys())):
                if list(self.schemas.keys())[i] == relationName:
                    return list(self.schemas.values())[i][attribute]
        else:
            raise AttributeError(errorMessage(attribute, "attribute", "String"))

    """Méthode qui vérifie si le type de value correspond au type de l'attribut
        Args:
            attribute (str): la colonne correspondant à la value
            value : valeur dont on veut vérifier si le type correspond bien à la colonne (attribut)
        Returns:
            bool: True si le type correspond, False sinon"""
    def checkType(self, attribute, value, relationName):
        attributeType = self.getAttributeType(attribute, relationName)
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
        listTab = []
        namesTitle = "| Names |"
        typesTitle = "| Types |"
        for i in range(len(list(self.schemas.keys()))):
            subSchema = list(self.schemas.values())[i]
            names = namesTitle
            types = typesTitle
            for j in range(len(list(subSchema.keys()))):
                name = list(subSchema.keys())[j]
                typ = list(subSchema.values())[j]
                if len(name) > len(typ):
                    names += name + " |"
                    types += typ + " "*(len(name)-len(typ)) + " |"
                else:
                    names += name + " "*(len(typ)-len(name)) + " |"
                    types += typ + " |"
            borders = "-"*len(names)
            l = int((len(names)-len(namesTitle))/2) - int((len(list(self.schemas.keys())[i]))/2)-1
            title = "|" + " "*l + list(self.schemas.keys())[i] + " "*l + "|"
            res = " "*len(namesTitle) + "-"*len(title)+"\n"+" "*len(namesTitle)+title+"\n"+borders+"\n"+names+"\n"+types+"\n"+borders
            listTab.append(res)

        finalRes = ""
        for tab in listTab:
            finalRes += tab+"\n"+""+"\n"
        return finalRes

    def __repr__(self):
        return __str__

class Table:
    def __init__(self, name, dbSchema):
        self.name = name
        self.dbSchema = dbSchema

class Rel:
    """Classe représentant une relation. C'est la classe mère des classes des expressions algébriques SPJRUD
        Args:
            dbSchema (DBSchema or Rel): schéma de base de données ou relation sur lequel la nouvelle relation va se baser
            dbSchema2 (DBSchema or Rel): schéma de base de données ou relation utilisé par les classes Join, Union, Diff
                                         qui travaillent sur plusieurs DBSchemas ou Relations simultanément (optionnel)"""
    def __init__(self, dbSchema1, dbSchema2 = None):
        #Vérification des types + copie du schéma de base de données pour ne pas modifier le schéma d'origine
        if isinstance(dbSchema1, DBSchema):
            """Vrai copie de DBSchema car certaines relations peuvent changer ce schema
                   et on ne veut pas avoir d'effet de bord"""
            self.dbSchema = copy.deepcopy(dbSchema1)

        elif isinstance(dbSchema1, Rel):
            self.dbSchema = copy.deepcopy(dbSchema1.dbSchema)

        else:
            raise ValueError(errorMessage(dbSchema1, "dbSchema1", "DBSchema or Rel"))

        #Utilisé par la jointure, l'union et la différence qui travaillent sur plusieurs relations simultanément
        if not dbSchema2 == None:
            if isinstance(dbSchema2, DBSchema):
                self.dbSchema2 = copy.deepcopy(dbSchema2)

            elif isinstance(dbSchema2, Rel):
                self.dbSchema2 = copy.deepcopy(dbSchema2.dbSchema)

            else:
                raise ValueError(errorMessage(dbSchema2, "dbSchema2", "DBSchema or Rel"))

    """Méthode qui renvoie un String correspondant à la requête SQL de la relation."""
    def toSql(self, delimiters=False):
        return self.dbSchema.name


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

        #On vérifie que l'attribut de l'opération est bien un attribut du DBSchema
        if not self.dbSchema.isAttribute(operation.param1):
            raise AttributeError('"' + operation.param1 + '"' + " in operation is not an attribute of the DBSchema")

        #Si le membre de droite de l'opération est aussi un attribut, on vérifie qu'il fait bien partie du DBSchema
        if isinstance(operation.param2, Attribute) and not self.dbSchema.isAttribute(operation.param2.attribute):
            raise AttributeError('"' + operation.param2 + '"' + " in operation is not an attribute of the DBSchema")

        #Si le membre de droite de l'opération est une constante, on vérifie que le type de cette constante correspond au type de l'attribut
        if isinstance(operation.param2, Const) and not self.dbSchema.checkType(operation.param1, operation.param2.const):
            raise ValueError("The type of operation.param2 doesn't correspond to attribute's type : " + str(self.dbSchema.getAttributeType(operation.param1)))

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel.toSql(True) + " where " + self.operation.toSql()


class Proj(Rel):
    """Classe représentant la Projection dans l'algèbre relationnelle (SPJRUD)
        Args:
            attributes (list of string): liste d'attributs qu'il faut garder lors de la projection
            rel (Rel): relation sur laquelle effectuer la projection"""
    def __init__(self, attributes, rel):
        super().__init__(rel)
        self.rel = rel
        if not isinstance(attributes, list):
            raise ValueError(errorMessage(attributes, "attributes", "list"))

        #On vérifie que chaque attribut de la liste d'attributs donnée en argument est bien un attribut du DBSchema
        for attribute in attributes:
            if not self.rel.dbSchema.isAttribute(attribute):
                raise ValueError(attribute + " is not an attribute in the DBSchema")

        #Mise à jour de la liste d'attributs pour ne garder que les attributs de liste donnée en argument
        self.dbSchema.attributes = {key : self.dbSchema.attributes[key] for key in attributes}
        self.attributes = attributes


    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select distinct "
        for attribute in self.attributes:
            res += attribute + ", "
        return res[:-2] + " from " + self.rel.toSql(True)



class Join(Rel):
    """Classe représentant la Jointure dans l'algèbre relationnelle (SPJRUD)
        Args:
            rel1 (Rel): relation de gauche de la jointure (rel1 x rel2)
            rel2 (Rel): relation de droite de la jointure (rel1 x rel2)"""
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)
        #On rajoute au dictionnaire d'attributs les attributs de rel2
        self.dbSchema.attributes.update(self.dbSchema2.attributes) #Vérifier que les colonnes ayant le m nom doivent avoir le m type
        self.rel1 = rel1
        self.rel2 = rel2

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " natural join " + "(select * from " + self.rel2.toSql(True)

class Rename(Rel):
    """Classe représentant le Rennomage dans l'algèbre relationnelle (SPJRUD)
        Args:
            attributeFrom (str): attribut à renommer
            attributeTo (str): String représentant le nouveau nom d'attributeFrom.
            rel (Rel): relation sur laquelle effectuer le renommage """
    def __init__(self, attributeFrom, attributeTo, rel):
        super().__init__(rel)
        self.rel = rel
        #On vérifie qu'attributeFrom est bien le nom d'un attribut du DBSchema
        if not self.dbSchema.isAttribute(attributeFrom):
            raise AttributeError(attributeFrom + " is not the name of an attribute")
        self.attributeFrom = attributeFrom

        if not isinstance(attributeTo, str):
            raise ValueError(errorMessage(attributeTo, "attributeTo", "String"))

        #On vérifie qu'attributeTo n'est pas déjà un nom d'attribut du DBSchema
        if self.dbSchema.isAttribute(attributeTo):
            raise AttributeError(attributeTo + " is already a name of another attribute")
        self.attributeTo = attributeTo

        #Ajout de l'attribut attributeTo au DBSchema
        self.dbSchema.attributes[attributeTo] = self.dbSchema.attributes[attributeFrom]
        #Supression de l'attribut attributeFrom au DBSchema
        del self.dbSchema.attributes[attributeFrom]

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select "
        for i in list(self.rel.dbSchema.attributes.keys()):
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
        if not set(self.dbSchema.attributes.keys()) == set(self.dbSchema2.attributes.keys()):
            raise SorteError("Sorte of rel1 must be equal to sorte of rel2")
        self.rel1 = rel1
        self.rel2 = rel2

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " union " + "(select * from " + self.rel2.toSql(True)

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
        return "select * from " + self.rel1.toSql(True) + " except " + self.rel2.toSql(True)


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

        if not (isinstance(param2, Const) and not isinstance(param2, Attribute)):
            raise ValueError(errorMessage(param2, "param2", "Const or Attribute"))
        self.param2 = param2

    def toSql(self):
        return str(self.param1) + self.symbol + str(self.param2)

class Eq(Operation):
    """Classe représentant l'équalité (=)"""
    def __init__(self, param1, param2):
        super().__init__("=", param1, param2)

class Greather(Operation):
    """Classe représentant l'opération (>)"""
    def __init__(self, param1, param2):
        super().__init__(">", param1, param2)

class GreatherOrEqual(Operation):
    """Classe représentant l'opération (>=)"""
    def __init__(self, param1, param2):
        super().__init__(">=", param1, param2)

class Less(Operation):
    """Classe représentant l'opération (<)"""
    def __init__(self, param1, param2):
        super().__init__("<", param1, param2)
        
class LessOrEqal(Operation):
    """Classe représentant l'opération (<=)"""
    def __init__(self, param1, param2):
        super().__init__("<=", param1, param2)



class Const:
    """Classe qui contient une constante, utilisé lors de la sélection"""
    def __init__(self, const):
        self.const = const

    def __str__(self):
        #On rajoute des "" si la constante est un String pour que la représentation en SQL soit correcte
        if isinstance(self.const, str):
            return '"' + self.const + '"'
        else:
            return str(self.const)

class Attribute:
    """Classe qui contient un nom d'attribut, utilisé lors de la sélection"""
    def __init__(self, attribute):
        self.attribute = attribute

    def __str__(self):
        return self.attribute

class AttributeError(Exception):
    def __init__(self, message):
        self.message = message

class SorteError(Exception):
    pass

class ExtentionError(Exception):
    def __init__(self, message):
        self.message = message

class SqlRequest:
    def __init__(self, message):
        if not isinstance(message, str):
            raise TypeError(errorMessage(message, "message", "string"))

        self.message = message

class Sql:
    def __init__(self, dbFile):
        if not isinstance(dbFile, str):
            raise TypeError(errorMessage(dbFile, "dbFile", "string"))
        if dbFile[-3:] != ".db":
            raise ExtentionError(extentionMessage(dbFile[-3:], ".db"))
        self.dbFile = dbFile
        self.dbSchema = self.getDBSchema()

    """
        Méthode permettant de récupérer le dbSchema d'une base de données
        Returns :
            Le dbSchema de la base de données
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
            attributes = {}
            for sub in res:
                t = sub.split(" ")
                if(t[0] != ""):
                    attributes[t[0]] = t[1]
                else:
                    attributes[t[1]] = t[2]
            db.addRelation(row[0], attributes)
        return db

    def do(self, request):
        if not isinstance(request, str):
            raise TypeError(errorMessage(request, "request", "string"))
        
        connexion = Sqlite3.connect(self.dbFile)
        cursor = connexion.cursor()
        cursor.execute(request)
        print(cursor.fetchall())
    
        

def extentionMessage(extention, correctExtetion):
    return "The extention must be "+str(correctExtention)+", however it is "+str(extention)+"."

def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + "."

def doSql(connexion, request, dbSchema):
    if not isinstance(connexion, sqlite3.Cursor):
        raise TypeError(errorMessage(connexion, "connexion", "sqlite3.Cursor"))
    if not isinstance(request, str):
        raise TypeError(errorMessage(request, "request", "string"))
    if not isinstance(dbSchema, DBSchema):
        raise TypeEroor(errorMessage(dbSchema, "dbSchema", "DBSchema"))
    result = connexion.execute(request)
    # Cette liste reprend la longueur max de chaque colonne
    listLenRes = []
    # Cette liste reprend le nom des colonnes restantes
    listRes = []
    
    for row in result:
        print(row)
        res = ""
        for i in range(len(row)):
            res += row[i]+" "
            try:
                maxLen = listLenRes[i]
                if len(row[i]) > maxLen:
                    listLenRes[i] = len(row[i])
            except IndexError:
                listLenRes.append(len(row[i]))
        listRes.append(res)
    print(listLenRes)
    print(listRes)
    

if __name__ == "__main__":
    conn = sqlite3.connect("testSql.db")
    connexion = conn.cursor()
    

    print(type(connexion))

    #connexion.execute("create table personne (nom text, prenom text, age int)")
    connexion.execute("insert into personne values ('Cassart', 'Justin', 21)")
    connexion.execute("insert into personne values ('Collin', 'Florent', 20)")
    connexion.execute("insert into personne values ('Huon', 'Cyril', 20)")

    connexion.execute("create table code (nom text, prenom text, language text)")
    connexion.execute("insert into code values ('Cassart', 'Justin', 'JAVA')")
    connexion.execute("insert into code values ('Cassart', 'Justin', 'PYTHON')")
    connexion.execute("insert into code values ('Collin', 'Florent', 'JAVA')")
    connexion.execute("insert into code values ('Collin', 'Florent', 'PYTHON')")
    connexion.execute("insert into code values ('Collin', 'Florent', 'C++')")
    # db = DBSchema("emp", {"A":"TEXT","B":"INT"})
    # a = Diff(Proj(["A"], Rel(db)), Select(Eq("A", Const("Jean")), Proj(["A"], Rel(db))))
    # print(a.toSql())
    #db = DBSchema("personne", {"nom":"TEXT", "prenom":"TEXT", "age":"INTEGER"})
    
    #print(a.toSql())
    #doSql(connexion, a.toSql(), db)

    db = DBSchema({"personnes" : {"Name" : "TEXT", "Surname" : "TEXT", "Age" : "INTEGER"},
                   "parents" : {"Name" : "INTEGER", "Surname" : "TEXT", "NumChildren" : "INTEGER"}})

    #sql = Sql("testSql.db")
    #db = sql.dbSchema
    a = Proj(["prenom","nom"], Select(Greather("age", Const(10)), Rel(db)))
    print(a.tosql())
    #sql.do(a)
