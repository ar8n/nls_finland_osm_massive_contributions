#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 17:01:22 2023

@author: alexysren
"""

"""
------------------------------------------------------------------------------

                    OSM FEATURE-LEVEL MODIFICATIONS ANALYSER
                                [BETA VERSION]
                    
                   * custom tool by Alexys Ren — July 2023 *


------------------------------------------------------------------------------
"""

"""
['BETA VERSION']: Since some challenges were encountered in our investigation, the initial goal was
rerouted toward statistics on geometry modification. However, all the functions below were implemented so to be easily developed further to achieve the initial goal.

For more information, please see my 'internship_report'!
"""

"""
[Quick start]: If you would like to use the code as it is, just modify the 'MAIN' part at the end of the code to fit your needs!
"""


### Libraries import

from tqdm import tqdm   # for displaying a progress bar on loops
import os

import psycopg2     # PostgreSQL driver for Python support
import pandas as pd
import numpy as np



### Functions


def osm_versions(osm_id):
    """
    Returns all the versions of a OSM feature based on its id

    Parameters
    ----------
    osm_id : int
        OSM FEATURE ID.

    Returns
    -------
    pandas DataFrame
        SQL QUERY RESULT.

    """
    cur.execute("SELECT * FROM history WHERE osm_id = %s ORDER BY version;", (osm_id,))
    return pd.DataFrame(cur.fetchall(), columns=['id',
                                                 'osm_id',
                                                 'osm_type',
                                                 'version',
                                                 'timestamp',
                                                 'uid',
                                                 'user',
                                                 'changeset_id',
                                                 'visible',
                                                 'lat',
                                                 'lon',
                                                 'members_refs',
                                                 'other_tags'])




def geometry_references_resolver(member_versions, feature_timestamp):
    """
    When resolving references for accessing ways and relations geometries,
    referenced members may also have several versions to be taken into account.
    
    This function takes all the versions of a referenced member from a way or a relation,
    and return a unique version from it.
    
    This unique version of the referenced member corresponds to 
    its latest version while being older than the feature's timestamp.

    Parameters
    ----------
    member_versions : pandas DataFrame
        CONTAINS ALL THE VERSIONS OF A REFERENCED MEMBER.
    feature_timestamp : pandas Timestamp
        TIMESTAMP FOR THE OSM FEATURE TO WHICH THE REFERENCE IS LINKED.

    Returns
    -------
    pandas Series
        UNIQUE REFERENCED MEMBER FOR THE WAY/RELATION.

    """
    
    # Calculating time difference
    member_versions['time_difference'] = member_versions['timestamp'] - feature_timestamp

    # Filtering versions older than 'feature_timestamp'
    filtered_df = member_versions[member_versions['timestamp'] <= feature_timestamp]
    
    n, m = filtered_df.shape
    if n == 0:
        # tests revealed that some referenced members can
        # be younger (in time) than the ways or relations they are referenced to, which
        # is theorically impossible: DATA error (Error handling in 'geometry_modification')
        return filtered_df
    
    # Find the version with minimal time difference from the filtered DataFrame
    min_time_difference_index = filtered_df['time_difference'].idxmax() # relative time!
    return member_versions.iloc[min_time_difference_index]






## Topology of modifications

# Geometry modification

def geometry_modification(t1,t2,osm_type):
    """
    [RECURSIVE FUNCTION]
    returns 1 if there has been a geometry modification, None if not,
    or 2 if there has been an error in the process

    Parameters
    ----------
    t1 : pandas Series
        PREVIOUS VERSION.
    t2 : pandas Series
        CURRENT VERSION.
    osm_type : string
        FEATURE TYPE.

    Returns
    -------
    int
        see main description.

    """
    
    if osm_type == 'node':
        if (t1.lat == t2.lat and t1.lon == t2.lon) or (np.isnan(t2.lat) and np.isnan(t2.lon)):
            # if coordinates are equals or if the geometry has been erased
            return None
        return 1
    
    # So frow now on, only ways and relations are considered
    t1_members_refs = t1['members_refs']
    t2_members_refs = t2['members_refs']
    
    if t2_members_refs == None:
        # if the way/relation geometry has been erased
        return None
    
    if t1_members_refs == None:
        # if the way/relation has no referenced members: DATA error
        #   -> it's theorically impossible since way and relation existences relies
        #       on their references!
        return 2
    
    n, m = len(t1_members_refs), len(t2_members_refs)
    
    if n != m:
        # if the numbers of nodes/members are not equal, there has been a modification!
        return 1
    
    t1_timestamp = t1['timestamp']
    t2_timestamp = t2['timestamp']
    

    if osm_type == 'way':
        # Iterating over referenced nodes
        for i in range(n):
            
            # Referenced nodes from ways may also have several versions needed to be taken into account
            t1_i_node_versions = osm_versions(t1_members_refs[i])
            t2_i_node_versions = osm_versions(t2_members_refs[i])
            
            # If the referenced node doesn't exist in our database
            #   -> cut during the geographical extraction of Otaniemi: EDGE EFFECT error
            if t1_i_node_versions.shape[0] == 0 or t2_i_node_versions.shape[0] == 0:
                return 2
            
            
            # Selecting the right version of the referenced node based on timestamps
            #print("t=", t1_i_node_versions['timestamp'], "timestamp=",t1_timestamp)
            t1_i_node = geometry_references_resolver(t1_i_node_versions, t1_timestamp)
            t2_i_node = geometry_references_resolver(t2_i_node_versions, t2_timestamp)
            
            if t1_i_node.shape[0] == 0 or t2_i_node.shape[0] == 0:
                # if there has been an error (see 'geometry_references_resolver' inline comments)
                return 2
            
            # Comparing nodes geometry
            if geometry_modification(t1_i_node,t2_i_node,'node') != None:
                return 1
        return None
    
    else: # if osm_type == 'relation':
        # Iterating over referenced nodes/ways/relations
        for i in range(n):
            
            # Referenced members from relations may also have several versions needed to be taken into account
            t1_i_member_versions = osm_versions(t1_members_refs[i])
            t2_i_member_versions = osm_versions(t2_members_refs[i])
            
            # If the referenced member doesn't exist in our database
            #   -> cut during the geographical extraction of Otaniemi: EDGE EFFECT error
            if t1_i_member_versions.shape[0] == 0 or t2_i_member_versions.shape[0] == 0:
                return 2
            
            # Selecting the right version of the referenced member based on timestamps
            t1_i_member = geometry_references_resolver(t1_i_member_versions, t1_timestamp)
            t2_i_member = geometry_references_resolver(t2_i_member_versions, t2_timestamp)
            
            
            if t1_i_member.shape[0] == 0 or t2_i_member.shape[0] == 0:
                # if there has been an error (see 'geometry_references_resolver' inline comments)
                return 2
            
            
            # Comparing ways or relations geometry
            
            t1_i_member_type = t1_i_member['osm_type']
            t2_i_member_type = t2_i_member['osm_type']
            
                # if the feature types are different between the compared members, there has been a modification!
            if t1_i_member_type != t2_i_member_type:
                return 1
            
            if geometry_modification(t1_i_member, t2_i_member, t1_i_member_type) != None:
                return 1
        return None
    

# Semantic modifications (not our case study, to be filled if needed!)

def tag_enrichment(t1,t2):
    # same etc.
    return None

def tag_suppression(t1,t2):
    return None

def tag_modification(t1,t2):
    return None



## Encoding modification types into sequence
"""

[IMPORTANT NOTE]: The below function was originally meant for preparing the clustering analysis
based on encoded sequences of modifications with semantic ones included. It is now used
for geometry-focused studies, but can be developed further on to fit your needs.

"""

def sequence_modifications(osm_id):
    """
    returns the encoded sequence of modifications for one OSM feature based on its id

    Parameters
    ----------
    osm_id : int
        OSM FEATURE ID.

    Returns
    -------
    seq : int list
        SEQUENCE OF MODIFICATIONS.

    """
    
    # Retrieving all the feature versions
    versions = osm_versions(osm_id)
    n, m = versions.shape # n = number of versions, m = number of fields
    
    if n < 1:
        # tests has shown that it may happen a OSM feature is not recorded in the history
        return [2]
    
    seq = [] # sequence of modifications
    
    
    # Retrieving the feature type (is it a node, a way, or a relation?)
    feature_type = versions['osm_type'][0]
    
    
    # Iterating over versions (Pairwise similarity comparing)
    for i in range(n-1):
        geometryModification = geometry_modification(versions.iloc[i], versions.iloc[i+1], feature_type)
        if geometryModification != None:
            seq.append(geometryModification)
            
        # tag_enrichment =
        # if tag_enrichment is not None:
            # seq.append(tag_enrichment(versions.iloc[i],versions.iloc[i+1]))
            
        # tag_suppression =
        # tag_modification =
   
        # .
        # .
        # . and so on...
        
    return seq




def nb_geometry_modification(osm_id):
    """
    based on the current state of the 'sequence_modifications' function,
    nb_geometry_modification returns the number of geometry modifications
    underwent by an input OSM feature based on its ID.

    Parameters
    ----------
    osm_id : int
        OSM FEATURE ID.

    Returns
    -------
    int
        NUMBER OF GEOMETRY MODIFICATIONS FOR THE INPUT FEATURE.

    """
    seq = sequence_modifications(osm_id)
    if 2 in seq:
        return None
    return len(seq)



def get_geometry_modification():
    """
    saves locally and returns a pandas DataFrame containing one line per feature with its
    associated number of geometry modifications. 

    Returns
    -------
    df : pandas DataFrame
        see main description.

    """
    # Retrieving OSM feature ids from 'nls_buildings_multipolygons'
    cur.execute("""SELECT * FROM

(SELECT CAST(osm_id AS bigint) FROM nls_buildings_multipolygons
UNION
SELECT CAST(osm_way_id AS bigint) FROM nls_buildings_multipolygons) AS temp

WHERE temp.osm_id IS NOT NULL;""")
    
    df = pd.DataFrame(cur.fetchall(), columns=['osm_id'])
    
    nbGeometryModification = []
    
    for osm_id in tqdm(df.osm_id):
        nbGeometryModification.append(nb_geometry_modification(osm_id))
    
    df['nb_geometry_modification'] = nbGeometryModification
    
    df.to_csv("nls_buildings_multipolygons_nb_geometry_modification.csv", float_format="%.3f", index_label="index", sep=" ")
    return df
        
        
## Typology of modifications: Statistics


def massive_contributions_extract(filename):
    # /!\ TO BE MODIFIED:
        # additional input: feature id list containing ids of NLS massive contributions
        # filter the below dataframe to return only features coming from NLS massive contributions
        # thus allowing proper statistics to answer our initial problem

    """ [IN REALITY]: this function is just about loading a .csv file, saved previously to avoid launching 'get_geometry_modification' at each code running
                    (let's say you want to display the statistics again later...).
                    
                    Indeed, 'get_geometry_modification' is the most time consuming in the whole process! (the whole history is scanned!) """
    df = pd.read_csv(os.path.join(os.getcwd(), filename), delimiter=" ")
    return df



def modifications_analyser(dataframe):
    """
    displays detailed statistics about modifications underwent by the input features
    (can be developed further on to include semantic modifications)

    Parameters
    ----------
    dataframe : pandas DataFrame
        OSM FEATURES WITH NUMBER OF GEOMETRY MODIFICATIONS.

    Returns
    -------
    None.

    """
    nb_features = dataframe.shape[0]
    nb_geometry_modification = dataframe['nb_geometry_modification']
    arr = np.array(nb_geometry_modification)
    
    nb_nan_values = nb_features - nb_geometry_modification.count()

    # Creating boolean masks for non-zero and non-'NaN' values
    non_zero_mask = arr != 0
    non_nan_mask = ~np.isnan(arr)
    # Counting the number of values that satisfy both conditions
    nb_non_zero_and_non_nan_values = np.count_nonzero(np.logical_and(non_zero_mask, non_nan_mask))
    
    # Creating a boolean mask for zero values
    zero_mask = arr == 0
    # Counting the number of zero values using the boolean mask
    nb_zero_values = np.count_nonzero(zero_mask)


    print("\n\n----------------  Geometry modification statistics ----------------\n")
    print("Feature count:", nb_features)
    print("Among the ", nb_features, "features")
    print("    -",nb_non_zero_and_non_nan_values,"underwent geometric modifications  (", round(100*nb_non_zero_and_non_nan_values/nb_features, 1), "% )")
    print("    -",nb_zero_values,"have not been modified  (", round(100*nb_zero_values/nb_features, 1), "% )")
    print("    -",nb_nan_values,"could not be analysed  (", round(100*nb_nan_values/nb_features, 1), "% )")
    print("*    Average number of geometry modifications per feature:", round(nb_geometry_modification.mean(), 1))
    print("*    Max number of geometry modifications:", nb_geometry_modification.max(), "\n")
    


### Code execution — 'MAIN'

if __name__ == '__main__':

    # # Connecting to PostgreSQL database (feel free to change the details to fit your needs)
    # conn = psycopg2.connect(database="uusimaa", user="postgres", password="postgres", host="localhost", port="5432")

    # # Opening a cursor to perform database operations
    # cur = conn.cursor()


    """ /!\ This code assumes the existence of a PostgreSQL table containing your OSM history data, which also need to respect a particular table structure.
            If you haven't already done so, please import your history data using the 'history_to_pgsql_enhanced.py' Python script, 
            then ensure that the SQL query in the 'cur.execute' method of 'osm_versions' function matches the name of your history table!  /!\
            
        N.B.: Modifying the SQL query in the code was a coding mistake,
              as I should have asked the user for a tablename input in this 'MAIN' directly to make this easier
    """

    # # Saving the number of geometry modifications for each feature belonging to 'nls_buildings_multipolygons' table on our computer
    # get_geometry_modification()

    # Importing the saved file (from 'get_geometry_modification')
    nls_buildings_multipolygons = massive_contributions_extract("nls_buildings_multipolygons_nb_geometry_modification.csv")

    # Displaying statistics on geometry modification
    modifications_analyser(nls_buildings_multipolygons)

    # # Closing communication with the database
    # cur.close()
    # conn.close()