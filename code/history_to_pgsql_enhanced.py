#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:42:19 2023

@author: alexysren
"""



"""
------------------------------------------------------------------------------

                    OSM HISTORY FILE IMPORT INTO POSTGRESQL
                            [ENHANCED VERSION]
                    
                   * custom tool by Alexys Ren — August 2023 *


------------------------------------------------------------------------------
"""

"""
User guideline: Please take a look at the 'MAIN' part at the end of this script,
                so to make the necessary changes to import your own OSM history file !
"""




### Librairies import

from tqdm import tqdm    # for displaying the elapsed time after launching the code, and the number of iterations per second
import psycopg2    # PostgreSQL driver for Python support
from psycopg2 import sql # for generating dynamically SQL queries (for choosing dynamically a table name)
import lxml.etree    # for processing XML data, using C-based libraries libxml2 and libxslt — superior to the native ElementTree API in terms of processing time and functionalities




### Code


def history_importer(osm_history_filename, pgsql_tablename='OSM_history'):
    """
    imports an OSM history file (XML) as a new table in the PostgreSQL database
    provided by the user in the 'MAIN'

    Parameters
    ----------
    osm_history_filename : string
        OSM history filename with its extension.
    pgsql_tablename : string
        Tablename for output PostgreSQL table.

    Returns
    -------
    None.

    """
    
    # Creating a new PostgreSQL table for hosting history data
    cur.execute(sql.SQL("""CREATE TABLE public.{}
(
    id bigint,
    osm_id bigint,
    osm_type text,
    version integer,
    "timestamp" timestamp without time zone,
    uid bigint,
    "user" text,
    changeset_id bigint,
    visible boolean,
    lat double precision,
    lon double precision,
    members_refs bigint[],
    other_tags hstore,
    PRIMARY KEY (id)
);""").format(sql.Identifier(pgsql_tablename)))
    
    # Making the previous change to the database persistent
    conn.commit()
    

    with open(osm_history_filename, "rb") as f:
        
        id_primary_key = 0
    
        
        print("\n\n Parsing history from XML, on-the-fly importing... ")
        print("\n N.B.: For reference, importing a 7.73GB history file took me 1:30h (around 22 million feature versions). Please also consider your memory capacity!")
        print("\n\n ** Ongoing process, please make sure the screen saver mode is disabled on your computer! (otherwise the kernel will be interrupted!) **")
        
        for event, child in tqdm(lxml.etree.iterparse(f)):
            
            
            osm_type = child.tag
            
            if osm_type == 'osm':
                # Ignore 'OSM' XML tag (metadata)
                continue
            
            if osm_type in ['node', 'way', 'relation']:
                
                try:
                    if osm_id == child.attrib['id'] and version == child.attrib['version']:
                        """ The closing XML tag for a OSM feature can either be '/>' or either like '</feature_type>'.
                        In the case it is '</feature_type>', we jump directly to the next iteration, as the iterparse function considers this closing XML tag as a OSM feature in its own right
                        with all the information related to the very previous feature """
                        continue
                except UnboundLocalError: # triggered only once at the first iteration since both 'osm_id' and 'version' are not initialised yet!
                    pass
                
                osm_id = child.attrib['id']
                version = child.attrib['version']

                id_primary_key += 1 # inserting a new id column so every record has a unique identifier
                                    # (one feature can have several versions using the same feature ID!)
                # every try/except below is meant to test if the related info exists!
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
            
                
                if osm_type == 'node': # if the OSM object is a node
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
                    
                
                
                # Saving currently available data on the current OSM feature in a list
                history_record = [id_primary_key, 
                                        osm_id,
                                        osm_type,
                                        version, 
                                        timestamp, 
                                        uid,
                                        user,
                                        changeset_id,
                                        visible,
                                        lat,
                                        lon]
                
                """
                    (A): The 'etree.iterparse' method from lxml read all tags and members related to any OSM feature before the feature itself. Below, we retrieve any additional information
                    about the current OSM feature, and append it to the end of 'history_record' list.
                """
                
                try:
                    if len(next_members_refs) > 0:
                        # If member references were read
                        history_record.append(next_members_refs)
                    else:
                        history_record.append(None)
                except UnboundLocalError: # triggered only once at the first iteration since 'next_members_refs' is not initialised yet!
                    history_record.append(None)
                    
                    
                try:
                    if len(next_additional_tags) > 0:
                        # If tags were read
                        history_record.append(next_additional_tags)
                    else:
                        history_record.append(None)
                except UnboundLocalError: # triggered only once at the first iteration since 'next_additional_tags' is not initialised yet!
                    history_record.append(None)
                
                
                """
                    No more information about the current OSM feature is contained in the XML file, we proceed with the insertion phase.
                """
                
                # SQL query for inserting the data
                # N.B.: the 'INSERT' operation is repeated over all the OSM feature versions in the OSM history file
                cur.execute(sql.SQL("""INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, hstore(%s));""").format(sql.Identifier(pgsql_tablename)), history_record)
                
                # Make the changes to the database persistent
                conn.commit()
                
                # Freeing the memory
                del history_record
                
                
                # Freeing memory and variable initialisation for the next iteration
                try:
                    del next_members_refs
                except NameError: # triggered only once at the first iteration since 'next_members_refs' is not initialised yet!
                    pass
                next_members_refs = []
                try:
                    del next_additional_tags
                except NameError: # triggered only once at the first iteration since 'next_additional_tags' is not initialised yet!
                    pass
                next_additional_tags = []
                
                
                
                
            
            else:
                # if the XML element is actually a OSM tag
                """
                    See (A).
                """
        
    
                # if the tag is a referenced member of a way or a relation 
                if osm_type == "nd" or osm_type == "member":
                    
                    member_id = int(child.attrib['ref'])
                    next_members_refs.append(member_id)
                    
                
                # if the tag is just a common attribute
                else:
                    
                    key = child.attrib['k']
                    value = child.attrib['v']
                    
                    tag = [key,value]
                    
                    next_additional_tags.append(tag)
                    
                    
            # Freeing the memory for the next iteration
            child.clear()

    print("\n\n Process completed! \n Your OSM history file content should now be appearing in your '", pgsql_tablename, "' PostgreSQL table!")




### Main — Code execution


if __name__ == '__main__':
    
    # Connecting to the PostgreSQL Database
    conn = psycopg2.connect(database="uusimaa", user="postgres", password="postgres", host="localhost", port="5432")
    
    # Opening a cursor to perform database operations
    cur = conn.cursor()
    
    # Selecting the OSM history file for import
    """
    Important prerequisite: if it happens that you have the raw .osh.pbf file,
                            please convert it to .osh format (XML) before launching this code
                            ('osmium cat' command can be used for that purpose)
    """
    #filename = 'otaniemi-history.osh'
    filename = 'uusimaa-history.osh'
    
    """ /!\ PLEASE MAKE SURE THIS CURRENT PYTHON FILE IS LOCATED IN THE SAME DIRECTORY AS YOUR
                            HISTORY FILE /!\
    """
    
    # Choosing a name for the output PostgreSQL table (optional, default='OSM_history')
    tablename = 'history'

    # Importing history file content
    history_importer(filename, tablename)
    
    
    # Close communication with the database
    cur.close()
    conn.close()


    