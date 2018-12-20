import unittest
from algebraToSql import *

class TestStringMethods(unittest.TestCase):

    def test_Select(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        R = Select(Eq("A", Const("Jean")), Rel(table))
        self.assertEqual(R.toSql(), 'select * from emp where A="Jean"')

    def test_Proj(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        R = Proj(["A","B"], Rel(table))
        self.assertEqual(R.toSql(), 'select distinct A, B from emp')

    def test_Join(self):
        table1 = Table("ab", {"A":"TEXT","B":"INTEGER"})
        table2 = Table("ac", {"A":"TEXT","C":"REAL"})
        R = Join(Rel(table1), Rel(table2))
        self.assertEqual(R.toSql(), 'select * from ab natural join (select * from ac)')

    def test_Rename(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        R = Rename("A", "C", Rel(table))
        self.assertEqual(R.toSql(), 'select A "C", B from emp')

    def test_Union(self):
        table1 = Table("ab", {"A":"TEXT","B":"INTEGER"})
        table2 = Table("ac", {"A":"TEXT","B":"INTEGER"})
        R = Union(Rel(table1), Rel(table2))
        self.assertEqual(R.toSql(), 'select * from ab union (select * from ac)')

    def test_Diff(self):
        table1 = Table("ab", {"A":"TEXT","B":"INTEGER"})
        table2 = Table("ac", {"A":"TEXT","B":"INTEGER"})
        R = Diff(Rel(table1), Rel(table2))
        self.assertEqual(R.toSql(), 'select * from ab except ac')


if __name__ == '__main__':
    unittest.main()
