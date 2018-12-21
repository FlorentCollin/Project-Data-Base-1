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
        self.assertEqual(R.toSql(), 'select * from ab union select * from ac')

    def test_Diff(self):
        table1 = Table("ab", {"A":"TEXT","B":"INTEGER"})
        table2 = Table("ac", {"A":"TEXT","B":"INTEGER"})
        R = Diff(Rel(table1), Rel(table2))
        self.assertEqual(R.toSql(), 'select * from ab except select * from ac')

    def test_SelectErr1(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        with self.assertRaises(ValueError):
            Select("a", Rel(table))

    def test_SelectErr2(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        with self.assertRaises(AttributeError):
            Select(Eq("a", Const("Jean")), Rel(table))

    def test_SelectErr3(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        with self.assertRaises(AttributeError):
            Select(Eq("A", Attribute("Jean")), Rel(table))

    def test_SelectErr4(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        with self.assertRaises(TypeError):
            Select(Eq("A", Const(1)), Rel(table))

    def test_ProjErr1(self):
        table = Table("emp", {"A":"TEXT","B":"INTEGER"})
        with self.assertRaises(ValueError):
            Proj('a', Rel(table))

if __name__ == '__main__':
    unittest.main()
