import logging
from utils import MongoWriter


COLLECTION_MAPPING = {
    'Status': 'statuses',
    'Comment': 'comments',
    'User': 'users'
}


class WeiboData(object):

    _data = dict()
    _keys = ()

    def __init__(self, data: dict):
        self._data['_id'] = data.get('id', 0)
        for key in self._keys:
            if key in self._data:
                continue
            elif key in data:
                self._data[key] = data[key]

    def get(self, key, *args, **kwargs):
        return self._data.get(key, *args, **kwargs)

    def get_data(self):
        return self._data

    def write(self, writer: MongoWriter):
        collection = COLLECTION_MAPPING[self.__class__.__name__]
        result = writer.write(self._data, collection=collection)
        if result == -1:
            logging.warning('Duplicate id (_id:{}) found in collection.'.format(self._data['_id']))
        else:
            logging.info('Data (_id:{}) inserted into {}'.format(result, collection))


class Status(WeiboData):

    _keys = 'created_at', 'text', 'textLength', 'source', 'favorited', 'truncated', 'pic_urls', 'geo', 'is_paid', \
           'annotations', 'reposts_count', 'comments_count', 'attitudes_count', 'isLongText', 'multi_attitude', \
           'hide_flag', 'visible', 'biz_ids', 'biz_feature', 'page_type', 'hasActionTypeCard', 'darwin_tags', \
           'hot_weibo_tags', 'text_tag_tips', 'userType', 'positive_recom_flag', 'content_auth', 'gif_ids', \
           'is_show_bulletin', 'comment_manage_info'

    def __init__(self, data: dict, write_obj: bool=False, writer: MongoWriter=None):
        if 'user' in data:
            user = User(data['user'])
            self._data['uid'] = user.get('_id', 0)
            if write_obj and writer:
                user.write(writer=writer)
        elif 'uid' in data:
            self._data['uid'] = data.get('uid', 0)
        if 'retweeted_status' in data:
            retweeted_status = Status(data=data['retweeted_status'])
            self._data['retweeted_id'] = retweeted_status.get('_id')
            if write_obj and writer:
                retweeted_status.write(writer=writer)
        super().__init__(data=data)


class User(WeiboData):

    _keys = 'screen_name', 'name', 'province', 'city', 'location', 'description', 'url', 'profile_image_url', \
            'cover_image', 'cover_image_phone', 'profile_url', 'domain', 'gender',  'created_at', 'geo_enabled',\
            'verified', 'insecurity', 'ptype', 'allow_all_comment', 'avatar_large', 'avatar_hd', 'verified_reason', \
            'verified_state', 'verified_level', 'verified_type_ext', 'has_service_tel', 'verified_contact_name', \
            'verified_contact_email', 'verified_contact_mobile', 'follow_me', 'like', 'like_me', 'online_status', \
            'bi_followers_count', 'lang', 'star', 'block_word', 'block_app', 'credit_score', 'user_ability', 'urank'

    def __init__(self, data: dict):
        super().__init__(data=data)


class Comment(WeiboData):

    _keys = ()

    def __init__(self, data: dict):
        super().__init__(data=data)