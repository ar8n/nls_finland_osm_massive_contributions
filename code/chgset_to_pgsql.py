#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 14:31:54 2023

@author: alexysren
"""


"""
------------------------------------------------------------------------------

                    OSM CHANGESET FILE IMPORT TO POSTGRESQL
                    
                   * custom tool by Alexys Ren — June 2023 *


------------------------------------------------------------------------------
"""


"""
Important prerequisite: if it happens that you have the raw .osm.bz2 file, please extract the .osm from it
            before launching this code
"""



### Librairies

from tqdm import tqdm

import xml.etree.ElementTree as ET

import psycopg2






### Changeset file import

""" /!\ PLEASE MAKE SURE THIS CURRENT PYTHON FILE IS LOCATED IN THE SAME DIRECTORY AS YOUR
                        CHANGESET FILE /!\
"""
tree = ET.parse('uusimaa-changesets.osm') # feel free to change the filename here to fit your needs





### Connecting to the PostgreSQL Database
# feel free again to change the connection details to fit your needs

conn = psycopg2.connect(database="uusimaa", user="postgres", password="postgres", host="localhost", port="5432")
cur = conn.cursor() # to perform database operations





### Code

root = tree.getroot()


changesets = []

print("\n\n Parsing changesets... ")

for child in tqdm(root):
    # every try/except below are meant to test if the related info exists!
    try:
        changeset_id = child.attrib['id']
    except KeyError:
        changeset_id = None
    try:
        created_at = child.attrib['created_at']
    except KeyError:
        created_at = None
    try:
        closed_at = child.attrib['closed_at']
    except KeyError:
        closed_at = None
    try:
        Open = child.attrib['open']
    except KeyError:
        Open = None
    try:
        user = child.attrib['user']
    except KeyError:
        user = None
    try:
        uid = child.attrib['uid']
    except KeyError:
        uid = None
    try:
        min_lat = child.attrib['min_lat']
    except KeyError:
        min_lat = None
    try:
        min_lon = child.attrib['min_lon']
    except KeyError:
        min_lon = None
    try:
        max_lat = child.attrib['max_lat']
    except KeyError:
        max_lat = None
    try:
        max_lon = child.attrib['max_lon']
    except KeyError:
        max_lon = None
    try:
        num_changes = child.attrib['num_changes']
    except KeyError:
        num_changes = None
    try:
        comments_count = child.attrib['comments_count']
    except KeyError:
        comments_count = None
    
    
    
    # Preparing the additional tags for storing in hstore format (if they exist!)
    
    additional_tags = []

    try:
        temp = child[0]
    except IndexError:
        additional_tags = None
    else:
        for tag in child:
            key = tag.attrib['k']
            value = tag.attrib['v']
            additional_tags.append([key,value])
        
    changesets.append((changeset_id,created_at,closed_at,Open,user,uid,min_lat,min_lon,max_lat,max_lon,num_changes,comments_count,additional_tags))
    
    

### Inserting the output data into PostgreSQL

# the targeted table structure has to match with the columns pattern below, and also the table name must be the same!
# feel free to change if needed!

print("\n\n Inserting changesets into database... ")

for changeset in tqdm(changesets):
    cur.execute("""INSERT INTO changesets VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, hstore(%s));""", changeset)

conn.commit() # Make the changes to the database persistent

# Close communication with the database
cur.close()
conn.close()


    