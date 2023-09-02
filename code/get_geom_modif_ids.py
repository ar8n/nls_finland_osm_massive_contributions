#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 10:52:14 2023

@author: alexysren
"""

### Libraries import

import os

import pandas as pd
import numpy as np


### Code


def get_geom_modif_ids(filename):
    """
    create a new .csv file containing the OSM feature ids of those that have
    underwent geometry modification from the input file

    Parameters
    ----------
    filename : string
        CSV filename with its extension.

    Returns
    -------
    None.

    """
    
    df = pd.read_csv(os.path.join(os.getcwd(), filename), delimiter=" ")
    
    osm_id = df['osm_id']
    nb_geometry_modification = df['nb_geometry_modification']
    arr = np.array(nb_geometry_modification)
    
    # Creating boolean masks for non-zero and non-'NaN' values
    non_zero_mask = arr != 0
    non_nan_mask = ~np.isnan(arr)
    # Retrieving index of values that satisfy both conditions
    non_zero_and_non_nan_values = np.nonzero(np.logical_and(non_zero_mask, non_nan_mask))
    
    osm_ids = pd.DataFrame(np.take(osm_id, non_zero_and_non_nan_values[0]), columns=["osm_id"])
    
    output_filename = filename[:-4] + "_geom_modif_ids.csv"
    osm_ids.to_csv(output_filename, float_format="%.3f", index_label="index", sep=" ")
    
    print("\n\nThe result has been successfully saved in '" + os.getcwd() + "/" + output_filename + "'!")
    


### Main

if __name__ == '__main__':
    
    filename = 'nls_buildings_multipolygons_nb_geometry_modification.csv'
    
    get_geom_modif_ids(filename)