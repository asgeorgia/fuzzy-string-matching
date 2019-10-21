import pandas as pd
import sqlite3
from pandas import DataFrame
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import OrderedDict

#Author: George Sarfo
#Desc: Fuzzy Matching POS Products with iheart_Jane Products
#Date: 2019/10/21

#Import Python Modules and use necessary functions from modules

from fuzzy_match_module import main_brain
import db_connection




#Suppress SettingWithCopyWarning in pandas
#In a production implementation several possible exceptions will be caught
#Warnings will be handled with Try Except and not suppressed
#For this purposes we suppress the warning

pd.set_option('mode.chained_assignment',None)


#Main Function to be run from command line to execute program

def main(pos_products):

    #pos_products will come from an event (e.g aws lambda) in a production environment
    #Config file will hold the matching score used = 70 in this case, but will be supplied by config file in production
    main_brain(pos_products, 70)



if __name__ == "__main__":

    #Use path of products.json here in production
    #or invoke a lambda fucntion
    main('products.json')



