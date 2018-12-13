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
    def __init__(self, colonne, relation):
        self.colonne = colonne
        self.relation = relation

    def do(self):
        c.execute("SELECT "+self.colonne+" from "+self.relation)
        print(c.fetchone())

class Select(Requete):
    def __init__(self, colonne, constante, relation):
        self.colonne = colonne
        self.constante = constante
        self.relation = relation

    def do(self):
        c.execute("select "+self.colonne+" from "+self.relation+" where "+self.colonne+" = "+self.constante)
        return c.fetchone()


class Join(Requete):
    def __init__(self, relation1, relation2):
        self.relation1 = relation1
        self.realtion2 = relation2

    def do(self):
        print()

def traitement(arg):
    print()

    
if __name__ == "__main__":
    # permet de récupérer ce qui est placer en argument
    argument = sys.argv
    print(argument)

    # req = Requete("at", "ar")
    
    conn = sqlite3.connect('test.db')

    c = conn.cursor()

    
    # c.execute('''CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)''')

    # c.execute("INSERT INTO stocks VALUES ('2006-01-05', 'BUY', 'RHAT', 100, 35.14)")

    arg = "proj(*,rel(stocks))"

    print(traitement(arg))
    

    proj = Proj("*", "stocks")
    print(proj.do())
    sel = Select("price", "35.14", "stocks")
    print(sel.do())
    sel = Select("price", "0", "stocks")
    print(sel.do())
    # print(proj.do())

    print()
    
    c.execute("SELECT * from stocks")
    print(c.fetchone())
    
    # print(c.fetchone())

    conn.commit()
    
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
"""

