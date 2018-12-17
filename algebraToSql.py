import copy

class DBSchema(object):
    """docstring for DBSchema."""
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

    def isAttribute(self, attribute):
        return attribute in self.attributes

    def add(self, colName, colType):
        self.attributes[colName] = colsType

    def getAttributeType(self, attribute):
        return self.attributes[attribute]

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

    def toSql(self, delimiters=False):
        return self.dbSchema.name


class Select(Rel):
    """docstring for Select."""
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
    def __init__(self, symbol):
        self.symbol = symbol

class Eq(Operation):
    def __init__(self, param1, param2):
        super().__init__("=")
        self.param1 = param1
        if not (isinstance(param2, Const) or isinstance(param2, Attribute)):
            raise ValueError(errorMessage(param2, "param2", "Const or Attribute"))
        self.param2 = param2

    def toSql(self):
        return str(self.param1) + self.symbol + str(self.param2)

class Const:
    def __init__(self, const):
        self.const = const
    def __str__(self):
        if isinstance(self.const, str):
            return '"' + self.const + '"'
        else:
            return self.const

class Attribute:
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
