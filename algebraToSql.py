
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


class Rel(object):
    """docstring for Rel."""
    def __init__(self, table):
        if not isinstance(table, str):
            raise ValueError("Argument : " + str(table) + ", must be a String")
        self.table = table


class Select(Rel):
    """docstring for Select."""
    def __init__(self, arg):
        super(Select, self).__init__()
        self.arg = arg

def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + ". But " + str(arg) + " is a " + str(type(arg)) + "."

a = DBSchema("lol",10,10)
