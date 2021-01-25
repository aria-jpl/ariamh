import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from xml.etree.ElementTree import Element


def get_one_geogrid(geo_grid_element: Element) -> dict:
    """Get dictionary of GCP metadata from Sentinel-1 xml metadata

    Parameters
    ----------
    geo_grid_element : Element
        Element from xml files within:
         './geolocationGrid/
         'geolocationGridPointList/'
         'geolocationGridPoint'

    Returns
    -------
    dict
       Dictionary ontaining all metadata in GCP. The keys are:

       'azimuthTime', 'slantRangeTime', 'line', 'pixel', 'latitude',
       'longitude', 'height', 'incidenceAngle', 'elevationAngle'
    """
    data = {child.tag: child.text
            for child in list(geo_grid_element)}
    return data


def get_data_from_one_xml(xml_file_name: str) -> pd.DataFrame:
    """
    Parameters
    ----------
    xml_file_name : str
        The relative path to the xml file in the annotation directory of the
        sentinel-1 data.

    Returns
    -------
    pd.DataFrame
        Obtain all GCP from relevant XML file and turn into dataframe.

        The columns are:

        'azimuthTime', 'slantRangeTime', 'line', 'pixel', 'latitude',
        'longitude', 'height', 'incidenceAngle', 'elevationAngle'
    """
    tree = ET.parse(xml_file_name)
    root = tree.getroot()
    geo_grid_list = root.findall('./geolocationGrid/'
                                 'geolocationGridPointList/'
                                 'geolocationGridPoint')
    data = list(map(get_one_geogrid, geo_grid_list))
    df = pd.DataFrame(data)
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    return df


def get_data_from_one_slc(slc_directory: str) -> pd.DataFrame:
    """
    Get concantenated dataframe of all GCPs within SLC for various
    polarizations and swaths within slc file.
    """
    xml_files = list(Path(slc_directory).glob('annotation/s1*-iw*.xml'))
    dfs = list(map(get_data_from_one_xml, xml_files))
    df_slc = pd.concat(dfs, axis=0)
    return df_slc


def get_envelope_from_all_slcs() -> dict:
    # Assume the data is in the current work directory
    safe_dirs = list(Path('.').glob('S1*_IW_SLC*.SAFE/'))
    dfs = list(map(get_data_from_one_slc, safe_dirs))
    df_all_slcs = pd.concat(dfs, axis=0)
    envelope = {'xmin': df_all_slcs.longitude.min(),
                'xmax': df_all_slcs.longitude.max(),
                'ymin': df_all_slcs.latitude.min(),
                'ymax': df_all_slcs.latitude.max()}
    return envelope
