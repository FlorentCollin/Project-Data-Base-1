import copy


class DBSchema(object):
    """Classe permettant de stocker le schéma d'une base de données contenant une seule relation
        Args:
            name (str): nom du schéma de la base de données
            attributes (dict of (String:String): attributs de la relation (autrement dit, les colonnes de la relation)"""
    def __init__(self, name, attributes):
        if not isinstance(name, str):
            raise ValueError(errorMessage(name, "name", "String"))
        if not isinstance(attributes, dict):
            raise ValueError(errorMessage(attributes, "attributes", "Dict"))
        if not all(isinstance(item, str) for item in list(attributes.keys())):
            raise ValueError(errorMessage(attributes, "attributes", "dict of String key and value"))
        if not all(isinstance(item, str) for item in list(attributes.values())):
            raise ValueError(errorMessage(attributes, "attributes", "dict of String key and value"))
        self.name = name
        self.attributes = attributes
    """Méthode qui renvoie True si l'attribut est bien un attribut de la relation, False sinon
        Args:
            attribute (str): attribut à tester
        Returns:
            bool: True si l'attribut est un attribut de la relation, faux sinon"""
    def isAttribute(self, attribute):
        return attribute in self.attributes

    def getAttributeType(self, attribute):
        return self.attributes[attribute]

    """Méthode qui vérifie si le type de value correspond au type de l'attribut
        Args:
            attribute (str): la colonne correspondant à la value
            value : valeur dont on veut vérifier si le type correspond bien à la colonne (attribut)
        Returns:
            bool: True si le type correspond, False sinon"""
    def checkType(self, attribute, value):
        attributeType = self.getAttributeType(attribute)
        if isinstance(value, int):
            return attributeType == "INTEGER"
        elif isinstance(value, str):
            return attributeType == "TEXT"
        elif isinstance(value, float):
            return attributeType == "REAL"
        else:
            return False

    def __str__(self):
        return str(self.name) + " " + str(self.attributes)

    def __repr__(self):
        return __str__


class Rel:
    """Classe représentant une relation. C'est la classe mère des classes des expressions algébriques SPJRUD
        Args:
            dbSchema (DBSchema or Rel): schéma de base de données ou relation sur lequel la nouvelle relation va se baser
            dbSchema2 (DBSchema or Rel): schéma de base de données ou relation utilisé par les classes Join, Union, Diff
                                         qui travaillent sur plusieurs DBSchemas ou Relations simultanément (optionnel)"""
    def __init__(self, dbSchema1, dbSchema2 = None):
        if isinstance(dbSchema1, DBSchema):
            """Vrai copie de DBSchema car certaines relations peuvent changer ce schema
                   et on ne veut pas avoir d'effet de bord"""
            self.dbSchema = copy.deepcopy(dbSchema1)
        elif isinstance(dbSchema1, Rel):
            self.dbSchema = copy.deepcopy(dbSchema1.dbSchema)
        else:
            raise ValueError(errorMessage(dbSchema1, "dbSchema1", "DBSchema or Rel"))

        if not dbSchema2 == None:
            if isinstance(dbSchema2, DBSchema):
                """Vrai copie de DBSchema car certaines relations peuvent changer ce schema
                       et on ne veut pas avoir d'effet de bord"""
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

        if not isinstance(operation.param1, str):
            raise ValueError(errorMessage(operation.param1, "operation.param1", "String"))

        if not self.dbSchema.isAttribute(operation.param1):
            raise AttributeError('"' + operation.param1 + '"' + " in operation is not an attribute of the DBSchema")

        if isinstance(operation.param2, Attribute) and not self.dbSchema.isAttribute(operation.param2.attribute):
            raise AttributeError('"' + operation.param2 + '"' + " in operation is not an attribute of the DBSchema")

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

        for i in attributes:
            if not self.rel.dbSchema.isAttribute(i):
                raise ValueError(i + " is not an attribute in the DBSchema")

        self.dbSchema.attributes = {k : self.dbSchema.attributes[k] for k in attributes}
        self.attributes = attributes


    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select "
        for i in self.attributes:
            res += i + ", "
        return res[:-2] + " from " + self.rel.toSql(True)



class Join(Rel):
    """Classe représentant la Jointure dans l'algèbre relationnelle (SPJRUD)
        Args:
            rel1 (Rel): relation de gauche de la jointure (rel1 x rel2)
            rel2 (Rel): relation de droite de la jointure (rel1 x rel2)"""
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)
        self.dbSchema.attributes.update(self.dbSchema2.attributes)
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
        if not self.dbSchema.isAttribute(attributeFrom):
            raise AttributeError(attributeFrom + " is not the name of an attribute")
        self.attributeFrom = attributeFrom

        if not isinstance(attributeTo, str):
            raise ValueError(errorMessage(attributeTo, "attributeTo", "String"))
        if self.dbSchema.isAttribute(attributeTo):
            raise AttributeError(attributeTo + " is already a name of another attribute")
        self.attributeTo = attributeTo

        self.dbSchema.attributes[attributeTo] = self.dbSchema.attributes[attributeFrom]
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
        if not set(self.dbSchema.attributes.keys()) == set(self.dbSchema2.attributes.keys()):
            raise SorteError("Sorte of rel1 must be equal to sorte of rel2")
        self.rel1 = rel1
        self.rel2 = rel2

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        return "select * from " + self.rel1.toSql(True) + " union " + "(select * from " + self.rel2.toSql(True)

class Diff(Union):
        """Classe représentant la Différence dans l'algèbre relationnelle (SPJRUD)
            Args:
                rel1 (Rel): relation de gauche de la différence (rel1 - rel2)
                rel1 (Rel): relation de droite de la différence (rel1 - rel2) """
    def __init__(self, rel1, rel2):
        super().__init__(rel1, rel2)

    def toSql(self, delimiters=False):
        if delimiters:
            return "(" + self.toSql(False) + ")"
        res = "select distinct * from " + self.rel1.toSql(True) + " where ("
        for i in self.dbSchema.attributes.keys():
            res += i + ", "
        return res[:-2] + ") not in " + self.rel2.toSql(True)


class Operation:
    """Classe représentant une opération parmi (=, >, >=, <, <=)
        Args:
            symbol (str): symbole de l'opération
            param1 (str) : membre de gauche de l'opération. param1 doit être le nom d'un attribut
            param2 (Const ou Attribute): membre de droite de l'opération. param2 doit être une constante ou un nom d'attribut"""
    def __init__(self, symbol, param1, param2):
        self.symbol = symbol
        self.param1 = param1
        if not (isinstance(param2, Const) or isinstance(param2, Attribute)):
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
        if isinstance(self.const, str):
            return '"' + self.const + '"'
        else:
            return self.const

class Attribute:
    """Classe qui contient un nom d'attribut, utilisé lors de la sélection"""
    def __init__(self, attribute):
        self.attribute = attribute
    def __str__(self):
        return self.attribute

class AttributeError(Exception):
    pass

class SorteError(Exception):
    pass

def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + "."

# db = DBSchema("emp", {"A":"TEXT","B":"INT"})
# a = Diff(Proj(["A"], Rel(db)), Select(Eq("A", Const("Jean")), Proj(["A"], Rel(db))))
# print(a.toSql())
