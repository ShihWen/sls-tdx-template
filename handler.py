from helper import get_road_data, get_existing_file


def tdx_line_string_file_list(event, context):
    event['tdx_line_string_files'] = get_existing_file(data_type='line-string')

    return event


def line_string_road(event, context):
    result = get_road_data(category='road',
                           output_folder='line-string')
    event['num_of_road'] = result['num_of_road']
    event['ingest_dt_road'] = result['ingest_dt']

    return event