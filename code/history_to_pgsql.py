#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:42:19 2023

@author: alexysren
"""



"""
------------------------------------------------------------------------------

                    OSM HISTORY FILE IMPORT INTO POSTGRESQL
                    
                   * custom tool by Alexys Ren — July 2023 *


------------------------------------------------------------------------------
"""


"""
Important prerequisite: if it happens that you have the raw .osh.pbf file,
                        please convert it to .osh format (XML) before launching this code
                        (osmium cat command can be used for that purpose)
"""



### Librairies

from tqdm import tqdm

import xml.etree.ElementTree as ET

import psycopg2






### History file import

""" /!\ PLEASE MAKE SURE THIS CURRENT PYTHON FILE IS LOCATED IN THE SAME DIRECTORY AS YOUR
                        HISTORY FILE /!\
"""
print("\n\n Parsing XML... ")
tree = ET.parse('otaniemi-history.osh') # feel free to change the filename here to fit your needs





### Connecting to the PostgreSQL Database
# feel free again to change the connection details to fit your needs

conn = psycopg2.connect(database="uusimaa", user="postgres", password="postgres", host="localhost", port="5432")
cur = conn.cursor() # to perform database operations





### Code

root = tree.getroot()


history_records = []
id_primary_key = 0


print("\n\n Parsing history... ")

for child in tqdm(root):
    id_primary_key += 1 # inserting a new id column so every record has a unique identifier
                        # (one feature can have several versions using the same feature ID!)
    # every try/except below are meant to test if the related info exists!
    try:
        osm_id = child.attrib['id']
    except KeyError:
        osm_id = None
    osm_type = child.tag
    try:
        version = child.attrib['version']
    except KeyError:
        version = None
    try:
        timestamp = child.attrib['timestamp']
    except KeyError:
        timestamp = None
    try:
        uid = child.attrib['uid']
    except KeyError:
        uid = None
    try:
        user = child.attrib['user']
    except KeyError:
        user = None
    try:
        changeset_id = child.attrib['changeset']
    except KeyError:
        changeset_id = None
    try:
        visible = child.attrib['visible']
    except KeyError:
        visible = None

    
    if child.tag == 'node': # if the OSM object is a node
        try:
            lat = child.attrib['lat']
        except KeyError:
            lat = None
        try:
            lon = child.attrib['lon']
        except KeyError:
            lon = None
    else:
        lat = None
        lon = None
    
        
    
    
    
    
    
    # Preparing the additional tags for storing in hstore format (if they exist!)
    # and Saving the node references for the ways, and the member (node, way, or relation) references for the relations
    #     --> In fact, geometry information is only contained in nodes. So to access the geometry
    #         of a way, you need the geometries of the nodes that are part of this way. The same goes
    #         for the geometries of relations.
    
    additional_tags = None
    members_refs = None # references to nodes for ways and to members for relations
    
    # For initialising the storage list (see below)
    stop1 = True
    stop2 = True

    # Checking if the OSM object has additional tags
    try:
        temp = child[0]
    except IndexError:
        pass
    else:
        for tag in child:
            
            # if the tag is a referenced member of a way or a relation 
            if (child.tag == "way" and tag.tag == "nd") or (child.tag == "relation" and tag.tag == "member"):
                
                if stop1: # if it's the first referenced member encountered, initialise an empty list for storing
                    members_refs = []
                    stop1 = False
                    # NB: this initialisation won't happen again, since 'stop1' variable is not used anymore from now on
                    
                members_refs.append(int(tag.attrib['ref'])) # adding the member id to the list
            
            # if the tag is just a common attribute
            else:
                
                if stop2: # if it's the first additional tag encountered, initialise an empty list for storing
                    additional_tags = []
                    stop2 = False
                    # NB: same remarks as for 'stop1' variable
                
                key = tag.attrib['k']
                value = tag.attrib['v']
                additional_tags.append([key,value])
        
    history_records.append((id_primary_key, 
                            osm_id,
                            osm_type,
                            version, 
                            timestamp, 
                            uid,
                            user,
                            changeset_id,
                            visible,
                            lat,
                            lon,
                            members_refs,
                            additional_tags))
    
    

### Inserting the output data into PostgreSQL

# the targeted table structure has to match with the columns pattern below, and also the table name must be the same!
# feel free to change if needed!

print("\n\n Inserting history into database... ")

for history_record in tqdm(history_records):
    cur.execute("""INSERT INTO otaniemi_history VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, hstore(%s));""", history_record)

conn.commit() # Make the changes to the database persistent

# Close communication with the database
cur.close()
conn.close()


    