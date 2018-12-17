import copy
from types import *

class DBSchema(object):
    """docstring for DBSchema."""
    def __init__(self, name, colsName, colsType):
        if not isinstance(name, str):
            raise ValueError(errorMessage(name, "name", "String"))
        if not isinstance(colsName, list) and not all(isinstance(item, str) for item in colsName):
            raise ValueError(errorMessage(colsName, "colsName", "list of String"))
        if not all(isinstance(item, str) for item in colsType):
            raise ValueError(errorMessage(colsType, "colsType", "list of String"))
        self.name = name
        self.colsName = colsName
        self.colsType = colsType

    def add(self, colName, colType):
        self.colsName.append(colName)
        self.colsType.append(colType)

    def copy(self):
        return DBSchema(self.name, self.colsName, self.colsType)
		
    def __str__(self):
        return str(self.name)+" "+str(self.colsName)+" "+str(self.colsType)
		
    def __repr__(self):
        return __str__

    def getColType(self, col):
        for i in range(len(self.colsName)):
            if self.colsName[i] == col:
                return self.colsType[i]

class Operation:
    def __init__(self, table):
        if not isinstance(table, DBSchema) and not isinstance(table, Operation):
            raise ValueError(errorMessage(table, "table", "DBSchema or Operation"))
        self.table = table
		
    def getTable(self):
        if isinstance(self.table, Operation):
            return self.table.getTable()
        return self.table

class Select(Operation):
    """docstring for Select."""
    def __init__(self, table, col, value):
        super().__init__(table)
        if not isinstance(col, str):
            raise ValueError(errorMessage(col, "col", "String"))
        if not colIsInTable(col, table):
            raise columnError("The column "+str(col)+" is not in the table "+str(table))
        if not checkType(value, col, table):
            raise columnError("The type of "+str(value)+" is not equal to the type of '"+str(col)+"' ("+table.getColType(col)+")")


class columnError(Exception):
    def __init__(self, message):
        self.message = message

def colIsInTable(col, table):
    for column in table.colsName:
        if col == column:
            return True
    return False

def checkType(value, column, table):
    for i in range(len(table.colsName)):
        if table.colsName[i] == column and table.colsType[i] == getType(value):
            return True
    return False

class Eq:
    def __init__(self):
        print()


def getType(value):
    if isinstance(value, int):
        return "int"
    elif isinstance(value, str):
        return "str"
    elif isinstance(value, float):
        return "float"
    else:
        return "boolean"

def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + ". But " + str(arg) + " is a " + str(type(arg)) + "."

db = DBSchema("test", ["a"], ["str"])
a = Operation(Operation(db))
b = Operation(a)
d = b.getTable()
print(d)
a = 1
print(type(a))
db2 = db.copy()
db2.add("b", "int")
print(db)
print(db2)
Select(db, "a", 1)


