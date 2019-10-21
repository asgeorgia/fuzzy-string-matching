import pandas as pd
import sqlite3
from pandas import DataFrame
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import OrderedDict

#Import DB Connection module
import db_connection


def match_product(name, list_names, min_score):

    #Use -1 score for no match and return an empty name
    max_score = -1
    max_name = ""

    #go through existing iheart products to find a match
    for name2 in list_names:
        #Getting fuzzy score
        this_score = fuzz.token_set_ratio(name, name2)
        # Checking if we are above our threshold and have a better score
        if (this_score > min_score) & (this_score > max_score):
            max_name = name2
            max_score = this_score

    return (max_name, max_score)



def main_brain(pos_products, user_supplied_matching_score):

    #prepare database and load products
    #In a prod environment a config file will hold the path to products.csv
    #We use try except to handle exceptions
    #Single and General Exceptions was used, but in a prod environment each major block will have a separate try except
    #A robust log file will be written at the end of the day for reference
    #An email alert can be incorporated as well

    try:

        con=db_connection.prepare_sqlite_db_load_products('products.csv')

        cur = con.cursor()

        #Get iheart Jane Products from Jane DB into a dataframe and add the column names
        #Column names can also be in a config file in a production implementation
        cur.execute("SELECT * FROM products;")
        rows = cur.fetchall()
        iheart_products_df=DataFrame(rows, columns=['id','amount','brand','lineage','name','product_type','product_subtype', 'url'])
        #print(iheart_products_df.head(10))
        #iheart_products_df.columns=['id','amount','brand','lineage','name','product_type','product_subtype']

        #concatenate all the necessary columns of iheart_jane products that will be used for matching with pos_products
        iheart_products_df['concat_column']=iheart_products_df['name'].astype(str).str.replace('none','')+' '+iheart_products_df['lineage'].astype(str).replace('none','')+' '+iheart_products_df['brand'].astype(str).replace('none','') + ' '+ iheart_products_df['product_type'].astype(str).replace('none','')+' ' + iheart_products_df['product_subtype'].astype(str).replace('none','')

        # replace vape with vaporizer
        iheart_products_df['concat_column']=iheart_products_df['concat_column'].str.replace('vape', 'vaporizer')

        # lower case strings
        iheart_products_df['concat_column'] = iheart_products_df['concat_column'].astype(str).str.lower()

        #remove duplicates due to already concatenated attributes in the name column of the df
        iheart_products_df['no_duplicates'] = (iheart_products_df['concat_column'].str.split()
                                      .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                      .str.join(' '))

        #Read json POS products into a dataframe
        #In production copy the products to a historical table with timestamp for tracking and possible reprocessing
        
        json_df=pd.read_json(pos_products)

        #Filter Category and productName Columns from given pos_products to use for matching
        pos_name_category_df=json_df[['category', 'productName']]
        pos_name_category_df['category_name']=pos_name_category_df['category'] +' '+ pos_name_category_df['productName']

        pos_name_category_df['category_name']=pos_name_category_df['category_name'].astype(str).str.lower()
        pos_name_category_df['productName'] = pos_name_category_df['productName'].astype(str).str.lower()

        #We use Fuzzy string matching that uses Levenshtein Distance to match strings of words
        #As a general rule, we accept a threshold of 70% or more as a very good match
        #This can be modified as more and more insights are gained into the products matched

        dict_list = []
        # iterating over our players without salaries found above
        for name in pos_name_category_df.category_name:
            # Use our method to find best match, we can set a threshold here
            match = match_product(name, iheart_products_df.no_duplicates, user_supplied_matching_score)

            # New dict for storing data
            dict_ = {}
            dict_.update({"pos_product": name})
            dict_.update({"matched_iheart_product": match[0]})
            dict_.update({"match_score": match[1]})
            dict_list.append(dict_)

        matched_products_df = pd.DataFrame(dict_list)
        # Display results

        #join on jane products to get product ID in output

        matched_products_all=pd.merge(matched_products_df, iheart_products_df, how='left', left_on = 'matched_iheart_product', right_on = 'no_duplicates')

        matched_products_needed_fields=matched_products_all[['pos_product', 'matched_iheart_product', 'match_score', 'id']]

        matched_products_needed_fields['matched_jane_product_id']=matched_products_needed_fields['id']

        #print(matched_products_all)

        matched_products_needed_fields[['pos_product', 'matched_iheart_product', 'match_score', 'matched_jane_product_id']].to_csv('final_matches_output.csv')


        #print(matched_products_df)

    except Exception as ex:

        #Print exception to console
        #In production log exception to a log file
        print("Matching Failed "+str(ex))

