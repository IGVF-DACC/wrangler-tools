import argparse
import os
import requests

import pandas as pd
from collections import defaultdict

# prerequisite libraries:
# pip install pandas
# pip install openpyxl

# You will need to put API credentials in the environment variables: IGVF_API_KEY & IGVF_SECRET_KEY.

parser = argparse.ArgumentParser(
    prog='IGVF Metadata Table',
    description='Generates meatadata table for a given analysis set'
)

parser.add_argument('-a', '--accession', help='accession of one analysis set')

# will add this if useful
# parser.add_argument('-i', '--infile', help='A file containing a list of measurement set accessions.')

# api connection setting from environment variables.
# could add alternative ways
if os.getenv('IGVF_API_KEY', None) and os.getenv('IGVF_SECRET_KEY', None):
    auth = (os.getenv('IGVF_API_KEY'), os.getenv('IGVF_SECRET_KEY'))

url = 'https://api.data.igvf.org/'

# set the properties/columns here:
# any column with all rows = None will be dropped in final tables
basic_props = ['@id', 'accession', 'aliases', 'status', 'audit']
link_obj_props = {'input_file_sets': ['auxiliary-sets', 'samples', 'donors', 'library_construction_platform', 'assay_term', 'documents'], 
                 'auxiliary-set': ['samples', 'donors', 'library_construction_platform', 'documents'],
                'samples': ['sample_terms', 'biomarkers', 'modifications', 'sorted_from', 'donors', 'construct_library_sets', 'treatments', 'originated_from', 'sources', 'multiplexed_samples', 'demultiplexed_from', 'barcode_sample_map', 'targeted_sample_term', 'cell_fate_change_treatments', 'cell_fate_change_protocol'],
                'donor': ['phenotypic_features', 'documents', 'sources'],
                'modification': ['tagged_protein', 'documents','sources'],
                'treamtment': ['documents', 'sources'],
                # curated-sets?
}

#excluding linkTo props here
output_props = {'modifications': [ 'summary'],
                'treatments': ['summary'],
                'samples': ['sorted_from_detail'],
                'construct_library_sets': ['summary', 'guide_type', 'guide library'],
                'donors': [ 'summary'],
                'assay_term': ['term_name'],
                'sorted_from': ['construct_library_sets'], #how far should sorted_from samples go?
                'files': ['file_format', 'file_size', 'content_type']
                    }

def get_props_from_ids(obj_ids, prop_fields, prefix): 
    # each obj_id can contain multiple ids, e.g. /modifications/XXX, /modifications/YYY
    # and need to be in a str
    props_dict = defaultdict(list)
    obj_json_dict = dict() # store temporary json objs here to accelerate the query when there are many duplicated obj_ids
    for obj_id in obj_ids:
        if obj_id is None:
            for p in prop_fields:
                props_dict[prefix + '.' + p].append(None)
            continue

        if len(obj_id.split(', ')) == 1:
            if obj_json_dict.get(obj_id) is None:
                obj_json = requests.get(url+obj_id, auth = auth).json()
                obj_json_dict[obj_id] = obj_json
            else:
                obj_json = obj_json_dict[obj_id]
        
            for p in prop_fields:
                props_dict[prefix + '.' + p].append(obj_json.get(p))
        
        else: # that row has multiple ids -> concat props for each id into one array;
            for p in prop_fields:
                p_list = []
                for _id in obj_id.split(', '):
                    if obj_json_dict.get(_id) is None:
                        obj_json = requests.get(url+_id, auth = auth).json()
                        obj_json_dict[_id] = obj_json
                    else:
                        obj_json = obj_json_dict[_id]
                    
                    p_list.append(obj_json.get(p))
                props_dict[prefix + '.' + p].append(p_list)
        
    return props_dict

# For linkTo objs, do a secondary query with their @ids to get their properties (thus not relying on the embedded properties)
# this currently doesn't work if obj_id has multiple ids, leave it here for now because we might not want to expand the table too much,
# and that row could be in a separte table
# e.g. curated-set IGVFDS6983MMSZ -> multiple sample ids -> breaks when expand those ids to all linkTo obj ids

def get_link_prop_ids_from_ids(obj_ids, prop_fields, prefix):
    props_dict = defaultdict(list)
    for obj_id in obj_ids:
        if obj_id is None:
            for p in prop_fields:
                props_key = '.'.join([prefix, p, '@id'])
                props_dict[props_key].append(None)
            continue

        obj_json = requests.get(url+obj_id, auth = auth).json()
        for p in prop_fields:
            props_key = '.'.join([prefix, p, '@id'])
            if obj_json.get(p) is None:
                props_dict[props_key].append(None)
            elif isinstance(obj_json.get(p), list):
                if isinstance(obj_json.get(p)[0], dict):
                    l = ', '.join(i['@id'] for i in obj_json.get(p)) # concat list of ids to string
                    props_dict[props_key].append(l)
                else:
                    props_dict[props_key].append(', '.join(obj_json.get(p))) # for cases e.g. sample -> donors is an array of str ["/rodent-donors/IGVFDO3898MNLZ/"]
            else:
                if isinstance(obj_json.get(p), dict):
                    props_dict[props_key].append(obj_json.get(p)['@id'])
                else: # a str
                    props_dict[props_key].append(obj_json.get(p))
        
    return props_dict

# set conditional formatting on cell colors
# this could be optional from args
def status_color(x):
    if x == 'released':
        color = 'lightgreen'
    elif x == 'in progress':
        color = 'grey'
    else:
        color= 'white'

    return f'background-color: {color}'
def audit_color(x):
    if x is None: # missing audits
        color = 'red'
    elif len(x) == 0:
        color = 'lightgreen'
    else:
        color= 'white'
    
    return f'background-color: {color}'

def main():
    args = parser.parse_args()
    data_accession = 'analysis-sets/' + args.accession
    df_all_out = {}

    fileset_json = requests.get(url+data_accession, auth = auth).json()
    input_file_sets_ids = [f['@id'] for f in fileset_json['input_file_sets']]
    print ('Getting input_file_sets properties...')
    input_file_sets_dict = get_props_from_ids(input_file_sets_ids, basic_props + ['summary'], 'input_file_set')
    df_0 = pd.DataFrame(input_file_sets_dict)

    print ('Getting input_file_sets linkTo obj ids...')
    # looking for the same set of linkTo props defined in link_obj_props for all input_file_sets
    # TODO: might need to adjust for curated-sets
    input_file_sets_link_ids_dict = get_link_prop_ids_from_ids(input_file_sets_ids, link_obj_props['input_file_sets'], 'input_file_set')
    df_1 = pd.DataFrame(input_file_sets_link_ids_dict)
    
    print ('Getting input_file_sets linkTo obj properties...')
    input_file_sets_link_objs_dict = {}
    for k in input_file_sets_link_ids_dict.keys(): # e.g. k = donors
    # skip those linkto objs if all input file sets have None in that field
        if any(v is not None for v in input_file_sets_link_ids_dict[k]):
            obj_type = k.split('.')[-2]
            props_list = basic_props if output_props.get(obj_type) is None else basic_props + output_props[obj_type]
            props_dict = get_props_from_ids(input_file_sets_link_ids_dict[k], props_list, k.replace('.@id',''))
            # could output to a separate dataframe here for any linkTo obj
            input_file_sets_link_objs_dict.update(props_dict)     
    df_2 = pd.DataFrame(input_file_sets_link_objs_dict)

    df_out_1 = pd.concat([df_0, df_1, df_2], axis=1).dropna(how='all', axis=1)
    # drop duplicated columns, otherwise will cause issues when applying mapping to colors
    # could avoid this by refining the prop dicts
    df_out_1 = df_out_1.loc[:, ~df_out_1.columns.duplicated()]
    # conditional formatting on any column of audit & status
    df_out_1 = df_out_1.style\
        .applymap(audit_color, subset = [i for i in list(df_out_1.columns) if i.endswith('audit')])\
        .applymap(status_color, subset = [i for i in list(df_out_1.columns) if i.endswith('status')])
    df_all_out['input_file_sets'] = df_out_1

    print ('Getting input_file_sets samples properties...')
    if any(v is not None for v in input_file_sets_link_ids_dict['input_file_set.samples.@id']):
        samples_link_ids_dict = get_link_prop_ids_from_ids(input_file_sets_link_ids_dict['input_file_set.samples.@id'], link_obj_props['samples'], 'input_file_set.samples')
        samples_link_objs_dict = {}
        for k in samples_link_ids_dict.keys():
            if any(v is not None for v in samples_link_ids_dict[k]):
                obj_type = k.split('.')[-2]
                props_list = basic_props if output_props.get(obj_type) is None else basic_props + output_props[obj_type]
                props_dict = get_props_from_ids(samples_link_ids_dict[k], props_list, k.replace('.@id',''))
                samples_link_objs_dict.update(props_dict)
        df_3 = pd.DataFrame(samples_link_objs_dict)

        df_out_2 = pd.concat([df_0[['input_file_set.@id']],df_3], axis=1).dropna(how='all', axis = 1)
        df_out_2 = df_out_2.style\
            .applymap(audit_color, subset = [i for i in list(df_out_2.columns) if i.endswith('audit')])\
            .applymap(status_color, subset = [i for i in list(df_out_2.columns) if i.endswith('status')])

        df_all_out['input_file_sets.samples'] = df_out_2

    print ('Getting analysis-set samples properties...')
    # samples under the analysis-set
    if fileset_json.get('samples') is not None:
        samples_ids = [f['@id'] for f in fileset_json['samples']]
        samples_dict = get_props_from_ids(samples_ids, basic_props + output_props['samples'], 'analysis_set.samples')
        df_0 = pd.DataFrame(samples_dict)

        print ('Getting analysis-set samples linkTo obj ids...')
        samples_link_ids_dict = get_link_prop_ids_from_ids(samples_ids, link_obj_props['samples'], 'analysis_set.samples')
        samples_link_objs_dict = {}
        print ('Getting analysis-set samples linkTo obj properties...')
        for k in samples_link_ids_dict.keys():
            if any(v is not None for v in samples_link_ids_dict[k]):
                obj_type = k.split('.')[-2]
                props_list = basic_props if output_props.get(obj_type) is None else basic_props + output_props[obj_type]
                props_dict = get_props_from_ids(samples_link_ids_dict[k], props_list, k.replace('.@id',''))
                samples_link_objs_dict.update(props_dict)
        df_4 = pd.DataFrame(samples_link_objs_dict)

        df_out_3 = pd.concat([df_0, df_4], axis=1).dropna(how='all', axis = 1)
        df_out_3 = df_out_3.loc[:, ~df_out_3.columns.duplicated()]
        df_out_3 = df_out_3.style\
            .applymap(audit_color, subset = [i for i in list(df_out_3.columns) if i.endswith('audit')])\
            .applymap(status_color, subset = [i for i in list(df_out_3.columns) if i.endswith('status')])

        df_all_out['analysis_set.samples'] = df_out_3

    # files derived from table
    # file ids from analysis_set -> files -> derived_from
    #print ('Getting analysis-set files properties...')
    #if fileset_json.get('files') is not None:
    #    files_ids = [f['@id'] for f in fileset_json['files']]
        
        # output two tables -> one will be just one file row; another is the derived file table

    # write dataframes to excel
    # ignore any column where all rows are None
    print ('Writing to excel tables: '+ args.accession + '_metadata.xlsx')
    with pd.ExcelWriter(args.accession + '_metadata.xlsx') as writer:
        for k in sorted(df_all_out.keys()):
            df_all_out[k].to_excel(writer, sheet_name = k,index=False, engine='xlsxwriter')


if __name__ == "__main__":
    main()