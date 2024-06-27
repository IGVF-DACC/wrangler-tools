import argparse
import os
import requests

import pandas as pd
from collections import defaultdict

# prerequisite libraries:
# pip install requests
# pip install pandas
# pip install openpyxl
# pip install jinja2

# You will need to put API credentials in the environment variables: IGVF_API_KEY & IGVF_SECRET_KEY.

parser = argparse.ArgumentParser(
    prog='IGVF Metadata Table',
    description='Generates meatadata table for a given analysis set or a list of file sets'
)

parser.add_argument('-a', '--accession', help='accession of one analysis set')

parser.add_argument(
    '-i', '--infile', help='A file containing a list of measurement set or auxiliary set @ids.')

# api connection setting from environment variables.
# could add alternative ways
if os.getenv('IGVF_API_KEY', None) and os.getenv('IGVF_SECRET_KEY', None):
    auth = (os.getenv('IGVF_API_KEY'), os.getenv('IGVF_SECRET_KEY'))

url = 'https://api.data.igvf.org/'

# set the properties/columns here:
# any column with all rows = None will be dropped in final tables
# can add 'summary' to basic_props when it's available for all objs
basic_props = ['@id', 'aliases', 'status', 'audit']
# removed modifications, treatments, donors linkTo objs (documents, sources), summary in output_props is probably enough?
# is 'documents' useful?
# for the file-set 'paired' with the input file-set (could be either measurement set or auxiliary set),
# skip a the query for their link sample, just output their ids
link_obj_props = {'input_file_sets': ['measurement_sets', 'auxiliary_sets', 'control_for', 'control_file_sets', 'samples', 'library_construction_platform', 'assay_term', 'documents'],
                  'samples': ['sample_terms', 'disease_terms', 'biomarkers', 'modifications', 'sorted_from', 'part_of', 'donors', 'construct_library_sets', 'treatments', 'originated_from', 'sources', 'multiplexed_samples', 'demultiplexed_from', 'barcode_sample_map', 'targeted_sample_term', 'cell_fate_change_treatments', 'cell_fate_change_protocol'],
                  'files': ['derived_from'],
                  'auxiliary_sets': ['library_construction_platform'],
                  'measurement_sets': ['assay_term', 'library_construction_platform']
                  }

# excluding linkTo props here
# the linkTo props in auxiliary_sets and measurement_sets here only
output_props = {'modifications': ['summary'],
                'treatments': ['summary'],
                'disease_terms': ['term_name'],
                'samples': ['classifications', 'embryonic', 'cellular_sub_pool', 'sorted_from_detail', 'publication_identifiers'],
                'construct_library_sets': ['summary', 'guide_type', 'guide library'],
                'donors': ['summary'],
                'assay_term': ['term_name'],
                # how far should sorted_from samples go?
                'sorted_from': ['construct_library_sets'],
                # should 'part_of' sample output more properties?
                'files': ['file_format', 'file_size', 'content_type', 'upload_status'],
                'input_file_sets': ['dbxrefs', 'protocols', 'multiome_size', 'summary', 'publication_identifiers']
                }

# reset empty audits to be None -> the column will be dropped if all rows have empty audits


def reset_empty_audits(props_dict):
    for k, v in props_dict.items():
        if k.endswith('audit'):  # each element in array v can be a list of dicts or a dict
            for index, audit in enumerate(v):
                if isinstance(audit, list):
                    if all(not a for a in audit):
                        props_dict[k][index] = None
                else:
                    if not audit:
                        props_dict[k][index] = None

    return props_dict


def get_props_from_ids(obj_ids, prop_fields, prefix):
    # each obj_id can contain multiple ids, e.g. /modifications/XXX, /modifications/YYY
    # and need to be in a str
    props_dict = defaultdict(list)
    # store temporary json objs here to accelerate the query when there are many duplicated obj_ids
    obj_json_dict = dict()
    for obj_id in obj_ids:
        if obj_id is None:
            for p in prop_fields:
                props_dict[prefix + '.' + p].append(None)
            continue

        if len(obj_id.split(', ')) == 1:
            if obj_json_dict.get(obj_id) is None:
                obj_json = requests.get(url+obj_id, auth=auth).json()
                obj_json_dict[obj_id] = obj_json
            else:
                obj_json = obj_json_dict[obj_id]

            for p in prop_fields:
                props_dict[prefix + '.' + p].append(obj_json.get(p))

        else:  # that row has multiple ids -> concat props for each id into one array;
            for p in prop_fields:
                p_list = []
                for _id in obj_id.split(', '):
                    if obj_json_dict.get(_id) is None:
                        obj_json = requests.get(url+_id, auth=auth).json()
                        obj_json_dict[_id] = obj_json
                    else:
                        obj_json = obj_json_dict[_id]

                    p_list.append(obj_json.get(p))
                props_dict[prefix + '.' + p].append(p_list)

    props_dict = reset_empty_audits(props_dict)

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

        obj_json = requests.get(url+obj_id, auth=auth).json()
        for p in prop_fields:
            props_key = '.'.join([prefix, p, '@id'])
            if not obj_json.get(p):  # None or empty array
                props_dict[props_key].append(None)
            elif isinstance(obj_json.get(p), list):
                if isinstance(obj_json.get(p)[0], dict):
                    # concat list of ids to string
                    l = ', '.join(i['@id'] for i in obj_json.get(p))
                    props_dict[props_key].append(l)
                else:
                    # for cases e.g. sample -> donors is an array of str ["/rodent-donors/IGVFDO3898MNLZ/"]
                    props_dict[props_key].append(', '.join(obj_json.get(p)))
            else:
                if isinstance(obj_json.get(p), dict):
                    props_dict[props_key].append(obj_json.get(p)['@id'])
                else:  # a str
                    props_dict[props_key].append(obj_json.get(p))

    return props_dict


def get_link_objs_df(query_ids, query_link_obj_props, props_prefix):
    df = pd.DataFrame()
    print('Getting ' + props_prefix + ' linkTo obj ids...')
    query_link_ids_dict = get_link_prop_ids_from_ids(
        query_ids, query_link_obj_props, props_prefix)
    print('Getting ' + props_prefix + ' linkTo obj properties...')
    query_link_objs_dict = {}
    for k in query_link_ids_dict.keys():
        # skip those linkto objs if all input file sets have None in that field
        if any(v is not None for v in query_link_ids_dict[k]):
            obj_type = k.split('.')[-2]
            props_list = basic_props if output_props.get(
                obj_type) is None else basic_props + output_props[obj_type]
            props_dict = get_props_from_ids(
                query_link_ids_dict[k], props_list, k.replace('.@id', ''))
            # could output to a separate dataframe here for any linkTo obj
            query_link_objs_dict.update(props_dict)

    df = pd.DataFrame(query_link_objs_dict)
    return df

# set conditional formatting on cell colors
# this could be optional from args


def status_color(x):
    if x == 'released':
        color = 'lightgreen'
    elif isinstance(x, list) and all(i == 'released' for i in x):
        color = 'lightgreen'
    elif x == 'in progress':
        color = 'grey'
    else:
        color = 'white'

    return f'background-color: {color}'

# highlighting the ones with no audits


def audit_color(x):
    if isinstance(x, dict) and len(x) == 0:
        color = 'lightgreen'
    else:
        color = 'white'

    return f'background-color: {color}'


def output_df(dfs):
    if len(dfs) == 1:
        df_out = dfs[0].dropna(how='all', axis=1)
    else:
        # concat the input dfs by columns, and drop any column where all rows = None
        df_out = pd.concat(dfs, axis=1).dropna(how='all', axis=1)
        # drop duplicated columns, otherwise will cause issues when applying mapping to colors, could avoid this by refining the prop dicts
        df_out = df_out.loc[:, ~df_out.columns.duplicated()]

    # conditional formatting on any column of audit & status
    df_out = df_out.style\
        .map(audit_color, subset=[i for i in list(df_out.columns) if i.endswith('audit')])\
        .map(status_color, subset=[i for i in list(df_out.columns) if i.endswith('status')])

    return df_out


def main():
    args = parser.parse_args()
    input_file_sets_ids = []
    outfile_prefix = ''
    if args.accession:
        outfile_prefix = args.accession
        data_accession = 'analysis-sets/' + args.accession
        fileset_json = requests.get(url+data_accession, auth=auth).json()
        input_file_sets_ids = [f['@id']
                               for f in fileset_json['input_file_sets']]
    elif args.infile:
        outfile_prefix = args.infile.split('.')[0]
        with open(args.infile, 'r') as f:
            for row in f:
                # only accept measurement-sets or auxiliary-sets for now, since it will use linked obj defined in link_obj_props['input_file_sets']
                if row.startswith('/measurement-sets/') or row.startswith('/auxiliary-sets/'):
                    input_file_sets_ids.append(row.strip().split('\t')[0])
    
    if not input_file_sets_ids:
        print ('No input file sets found.')
        return

    df_all_out = {}

    print('Getting input_file_sets properties...')
    input_file_sets_dict = get_props_from_ids(
        input_file_sets_ids, basic_props + output_props['input_file_sets'], 'input_file_set')
    df_0 = pd.DataFrame(input_file_sets_dict)

    df_link = get_link_objs_df(
        input_file_sets_ids, link_obj_props['input_file_sets'], 'input_file_set')

    #this is redundant in get_link_objs_df
    input_file_sets_link_ids_dict = get_link_prop_ids_from_ids(
        input_file_sets_ids, link_obj_props['input_file_sets'], 'input_file_set')

    # if there are measurement / auxiliary sets linked to the input file-sets,
    # add columns for their linkTo obj @ids, but not doing a secondary query to get their props for now
    df_measurement_link = pd.DataFrame()
    df_auxiliary_link = pd.DataFrame()

    measurement_ids = input_file_sets_link_ids_dict['input_file_set.measurement_sets.@id']
    df_measurement_samples_ids = pd.DataFrame(get_link_prop_ids_from_ids(
        input_file_sets_ids, ['samples'], 'input_file_set.measurement_sets'))
    if any(v is not None for v in measurement_ids):
        df_measurement_link = get_link_objs_df(
            measurement_ids, link_obj_props['measurement_sets'], 'input_file_set.measurement_sets')

    auxiliary_ids = input_file_sets_link_ids_dict['input_file_set.auxiliary_sets.@id']
    df_auxiliary_samples_ids = pd.DataFrame(get_link_prop_ids_from_ids(
        auxiliary_ids, ['samples'], 'input_file_set.auxiliary_sets'))
    if any(v is not None for v in auxiliary_ids):
        df_auxiliary_link = get_link_objs_df(
            auxiliary_ids, link_obj_props['auxiliary_sets'], 'input_file_set.auxiliary_sets')

    df_out = output_df([df_0, df_link, df_measurement_samples_ids,
                       df_measurement_link, df_auxiliary_samples_ids, df_auxiliary_link])
    df_all_out['input_file_sets'] = df_out

    samples_ids = input_file_sets_link_ids_dict['input_file_set.samples.@id']

    if any(v is not None for v in samples_ids):
        # samples from input file-sets
        df_sample_link = get_link_objs_df(
            samples_ids, link_obj_props['samples'], 'input_file_set.samples')

        df_out = output_df(
            [df_0[['input_file_set.' + i for i in basic_props]], df_sample_link])
        df_all_out['input_file_sets.samples'] = df_out

    if args.accession:
        # samples directly from the analysis-set
        if fileset_json.get('samples') is not None:
            print('Getting analysis-set samples properties...')
            samples_ids = [f['@id'] for f in fileset_json['samples']]
            samples_dict = get_props_from_ids(
                samples_ids, basic_props + output_props['samples'], 'analysis_set.samples')
            df_0 = pd.DataFrame(samples_dict)

            df_link = get_link_objs_df(
                samples_ids, link_obj_props['samples'], 'analysis_set.samples')

            df_out = output_df([df_0, df_link])
            df_all_out['analysis_set.samples'] = df_out

        # files derived from table
        # file ids from analysis_set -> files -> derived_from
        if fileset_json.get('files') is not None and fileset_json['files']:
            print('Getting analysis-set files properties...')
            files_ids = [f['@id'] for f in fileset_json['files']]
            files_dict = get_props_from_ids(
                files_ids, basic_props + output_props['files'], 'analysis_set.files')
            df_5 = pd.DataFrame(files_dict)

            df_out = output_df([df_5])
            df_all_out['analysis_set.files'] = df_out

            # if any file has derived_from files -> put those derived_from files in a separate table
            files_link_ids_dict = get_link_prop_ids_from_ids(
                files_ids, link_obj_props['files'], 'analysis_set.files')
            for f_id, d_files_ids in zip(files_ids, files_link_ids_dict['analysis_set.files.derived_from.@id']):
                if d_files_ids is not None:
                    print('Getting derived_from files table for file ' + f_id + '...')
                    derived_from_dict = get_props_from_ids(d_files_ids.split(
                        ', '), basic_props + output_props['files'], f_id.split('/')[-2] + '.derived_from')
                    df = pd.DataFrame(derived_from_dict)

                    df_out = output_df([df])
                    df_all_out[f_id.split('/')[-2] + '.derived_from'] = df_out

    # write dataframes to excel
    print('Writing to excel tables: ' + outfile_prefix + '_metadata.xlsx')
    with pd.ExcelWriter(outfile_prefix + '_metadata.xlsx') as writer:
        for k in sorted(df_all_out.keys()):
            print('Sheet ' + k, ', Total number of columns: ' +
                  str(len(list(df_all_out[k].columns))))
            print('Column names: ' + '\n'.join(list(df_all_out[k].columns)))
            df_all_out[k].to_excel(writer, sheet_name=k,
                                   index=False, engine='xlsxwriter')


if __name__ == '__main__':
    main()
