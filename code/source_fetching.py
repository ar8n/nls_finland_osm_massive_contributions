#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 14:48:43 2023

@author: alexysren
"""


### Imports

import os # to get our current directory path
import time # to compute the elapsed time for each function to return something

from tqdm import tqdm # make loops show a smart progress meter

from osmcha import changeset # OpenStreetMap Changeset Analyzer
import osmium # PyOsmium

import numpy as np # numpy
import pandas as pd # pandas library



### Global variables

current_directory_path = os.getcwd()



### Functions


class DataHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.elements = []
        
    def node(self, n):
        self.elements.append(["node", n.id, n.changeset])
        
    def way(self, w):
        self.elements.append(["way", w.id, w.changeset])
        
    def relation(self, r):
        self.elements.append(["relation", r.id, r.changeset])
        
        
    
def get_data(filepath):
    """
    imports data from an OSM data file and returns features id along its
    associated changeset id in a Panda Dataframe
    
    Parameter:
        filepath (str): path to OSM data file
        
    Return:
        (DataFrame): Panda DataFrame containing features id and associated changeset id
    """
    dataHandler = DataHandler()
    dataHandler.apply_file(filepath)
    col_names = ["type", "id", "changesetId"]
    return pd.DataFrame(dataHandler.elements, columns=col_names)
        


def get_source(changeset_id):
    """
    retrieves the source key value from an input changeset id
    
    Parameter:
        changeset_id (int): changeset id
        
    Return:
        (str): source key value 
    """
    ch = changeset.Analyse(changeset_id)
    return ch.source





def apply_source(data, out_filename="data_with_source.csv"):
    """
    add the source key values column to the input that corresponds to each feature in
    the input data
    the result is also saved in a .csv file in root
    
    Parameter:
        data (DataFrame): Panda DataFrame containing features id and associated changeset id
        out_filename (str): filename for the output file (with its extension!)
        
    Return:
        (DataFrame): Panda DataFrame with columns [type, id, changesetId, source] (in order)
    """
    
    print("\n\n\n-------------- Applying source process started... -------------- \n\n")
    start_time = time.time()
    
    
    # new_data = data[:50].copy()
    new_data = data.copy()
    source_column = np.array([])
    
    
    for i in tqdm(new_data.index):
        
        changeset_id = int(new_data.loc[i, "changesetId"])
        source = get_source(changeset_id)
        
        if source != "Not reported":
            source_column = np.append(source_column, source)
        else:
            source_column = np.append(source_column, "")
        
    new_data['source'] = source_column.tolist()

    

    new_data.to_csv(out_filename, float_format="%.3f", index_label='index', sep=" ")


    end_time = time.time()
    
    print("\n\n\n-------------- Job's finished! --------------\nElapsed time: ", round(end_time - start_time, 2), "seconds\n\n\n")
    
    return new_data
    




### Main


if __name__ == '__main__':
    
    # Data imports
    
    roads_filename = "roads-uusimaa.osm.pbf"
    buidlings_filename = "buildings-uusimaa.osm.pbf"
    
    roads_uusimaa = get_data(current_directory_path + "/" + roads_filename)
    
    # Source values retrieval
    
    roads_uusimaa_source = apply_source(roads_uusimaa, "roads-uusimaa_source.csv")