import sqlite3
import sys

class Rel:
    def __init__(self, relation):
        self.relation = relation

class Requete:
    def __init__(self, attribut, requete):
        self.attribut = attribut
        self.requete = requete
        print("requete")

    def __init__(attribut, relation):
        self.attribut = attribut
        self.relation = relation
        print("relation")

class Proj(Requete):
    def __init__(self, column, table):
        """if(colonne != "*"):
            try:
                if c.execute('''if exists(colonne)
                    ''')"""
        self.column = column
        self.table = table

    def do(self):
        for row in c.execute("select distinct "+self.column+" from "+self.table):
            print(row)


class Select(Requete):
    def __init__(self, column, value, table):
        self.column = column
        self.value = value
        self.table = table

    def do(self):
        if tableExists("selectTable"):
            c.execute("drop table selectTable")
        c.execute("create table selectTable as select "+self.column+
                         " from "+self.table+
                         " where "+self.column+" = "+self.value)
        return "selectTable"
        

class Join(Requete):
    def __init__(self, table1, table2):
        self.table1 = table1
        self.table2 = table2

    def do(self):   
        if tableExists("joinTable"):
            c.execute("drop table joinTable")
        c.execute("create table joinTable as select * from "+
                        self.table1+" natural join "+self.table2)
        return "joinTable"

class Rename(Requete):
    def __init__(self, column, name, table):
        self.column = column
        self.name = name
        self.table = table

    def do(self):
        if tableExists("renameTable"):
            c.execute("drop table renameTable")
        c.execute("create table renameTable as select "+self.column+" from "+
                  self.table)
        return "renameTable"

        

def traitement(arg):
    print()

def tableExists(tableName):
    tables = c.execute("select name from sqlite_master").fetchall()
    for table in tables:
        table = table[0]
        if table == tableName:
            return True
    return False

def showTables():
    tables = c.execute("select name from sqlite_master").fetchall()
    print(tables)

"""def showColumns(tableName):
    columns = PRAGMA table_info(+tableName+))
    print(columns)"""

    
if __name__ == "__main__":
    # permet de récupérer ce qui est placer en argument
    argument = sys.argv
    print(argument)

    # req = Requete("at", "ar")
    
    conn = sqlite3.connect('test.db')

    c = conn.cursor()

    """
    c.execute("drop table personne")
    c.execute("drop table parents")

    c.execute("create table personne (nom text, prenom text, age int)")
    c.execute("create table parents (nom text, prenom text, nombreEnfant int)")
    """
    
    c.execute("insert into personne values ('Cassart', 'Justin', 21)")
    c.execute("insert into personne values ('Courtecuisse', 'Christine', 53)")
    c.execute("insert into personne values ('Cassart', 'Rudy', 55)")
    c.execute("insert into personne values ('Cassart', 'Guillaume', 28)")
    c.execute("insert into personne values ('Cassart', 'Elodie', '18')")
    c.execute("insert into personne values ('Collin', 'Florent', '19')")

    c.execute("insert into parents values ('Courtecuisse', 'Christine', 3)")
    c.execute("insert into parents values ('Cassart', 'Rudy', 3)")

    #showColumns("personne")

    showTables()

    
    # c.execute('''CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)''')

    # c.execute("INSERT INTO stocks VALUES ('2006-01-05', 'BUY', 'RHAT', 100, 35.14)")

    #arg = "proj(*,rel(stocks))"

    #print(traitement(arg))

    # Proj("*", Join("personne", "parents").do()).do()

    #c.execute("select * from personne")
    #print(c.fetchone())

    #Proj("*", "personne").do()
    #Proj("*", "parents").do()
    """
    for row in c.execute('''select distinct pers.*, par.*
              from personne pers, parents par
              where pers.nom = par.nom
              and pers.prenom = par.prenom'''):
        print(row)

    for row in c.execute('''select distinct personne.nom, personne.prenom,
                            personne.age, parents.nombreEnfant
                            from personne
                            inner join parents on personne.nom = parents.nom
                            and personne.prenom = parents.prenom'''):
        print(row)
   
    for row in c.execute('''select distinct *
                            from personne
                            natural join parents
                            '''):
        print(row)

    Proj("*","personne")

    print()
    print()
    print("projection simple")
    Proj("*", "personne").do()

    print()
    print("jointure simple")
    #print(Join("personne", "parents").do())

    print()
    print("projection avec jointure")
    Proj("*", Join("personne", "parents").do()).do()
    
 

    for row in c.execute('''if exists(nom from personne)
                            select distinct nom from personne'''):
        print(row)
    
    # Permet de sauvegarder les modifications
    conn.commit()
    """
"""
Les requêtes : 
    S Select(colonne, valeur, relation)
    P Proj(colonne, relation)
    J Join(relation1, relation2) voir : https://sql.sh/cours/jointures
    R Ren(colonne, renommage, relation)
    U Union(relation1, relation2) Vérifier que sort(relation1) = sorte(relation2)
    D Dif(relation1, relation2) Vérifier que sort(relation1) = sorte(relation2)
Requete(attribut, relation)
Requete(attribut, requete)
Utilisation d'un node : avec le next on aurait d'office la suite facilement
Vérifier que la colonne appartienne à la relation : prendre colonne et relation faire un select et voir si erreur ou pas en sql
Vérifier qu'une colonne appartient à la table : if exists(...) => https://www.journaldunet.fr/web-tech/developpement/1202623-comment-verifier-qu-une-colonne-existe-dans-une-table-sql-server/
Les tables temporaires doivent avoir des noms différents en fonction de la requête car si on fait la jointure sur un renommage on va supprimer la table dans laquelle il faut travailler !
self.do() return l'objet et non ce que doit retourner do()
"""

