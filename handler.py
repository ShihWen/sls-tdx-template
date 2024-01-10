from helper import get_road_id, get_link_id


def get_tdx_road_id(event, context):
    event['get_tdx_road_id'] = get_road_id()
    return event


def get_tdx_link_id(event, context):
    event['get_tdx_link_id'] = get_link_id(city_list=['高雄市'])
    return event
