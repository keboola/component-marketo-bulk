'''
Created on 12. 11. 2018

@author: esner
'''
import unittest

from component import Component


class TestComponent(unittest.TestCase):


    def testRunEmptyFails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
