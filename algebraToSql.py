
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
		
	def __str__(self):
	    return str(self.name," ", self.colsName," ", self.colsType)

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
    def __init__(self, table, ):
        super().__init__(table)

def errorMessage(arg, argName, correctType):
    return "Argument " + str(argName) + " must be a " + str(correctType) + ". But " + str(arg) + " is a " + str(type(arg)) + "."

db = DBSchema("test", ["a"], ["str"])
a = Operation(Operation(db))
b = Operation(a)
d = b.getTable()
print(d)
