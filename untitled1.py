# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 21:59:52 2022

@author: onais
"""

import pypostalwin

parser = pypostalwin.AddressParser()
print("Hello")
parsedAddress = parser.runParser("The White House 1600 Pennsylvania Avenue NW, Washington, DC 20500, USA")
print(parsedAddress)
parser.terminateParser()