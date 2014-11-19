import sys
import logging
import time
import requests

from http_build_query import http_build_query


logger = logging.getLogger('hasoffers')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))


ROOT = 'http://api.hasoffers.com/v3/'


class Error(Exception):
    pass
class APIUsageExceededRateLimit(Error):
    pass


class Api(object):

    """
    Usage:
        client = Api(TOKEN, ID, debug=False, retry_count=1)
        response = client.call(target='Conversion', method='findAll', params={'limit': 10, 'contain': ['Offer']})
        response.extract_all()

    Short usage:
        client = Api(TOKEN, ID, debug=False, retry_count=1)
        client.Conversion.findAll(limit=10, contain=['Offer']).extract_all()

    More examples:
        offer = client.Offer.findById(id=1, contain=['Advertiser']).extract_one()

        print(offer.name)

        if offer.Advertiser:
            print(offer.Advertiser['id'])
    """

    class MethodProxy(object):

        def __init__(self, master):
            self.master = master
            self.target = None
            self.method = None

        def __call__(self, **kwargs):
            return self.master.call(target=self.target, method=self.method, params=kwargs)

        def __getattr__(self, method):
            self.method = method
            return self

    def __init__(self, network_token, network_id, debug=False, retry_count=1):
        self.network_token = network_token
        self.network_id = network_id

        if debug:
            self.level = logging.INFO
        else:
            self.level = logging.DEBUG

        self.retry_count = retry_count

        self.method_proxy = self.MethodProxy(self)

    def call(self, target, method, params=None):
        request = self.create_request(target, method, params)

        return self.send_request(request)

    def create_request(self, target, method, params):
        _params = {
            'NetworkId': self.network_id,
            'NetworkToken': self.network_token,
            'Method': method
        }
        _params.update(params or {})

        return Request(target, method, _params)

    def send_request(self, request):

        request.attempts += 1

        self.log('Executing %s' % request.url)
        start = time.time()

        http_response = requests.get(request.url)

        complete_time = time.time() - start
        self.log('Received %s in %.2fms: %s' % (http_response.status_code, complete_time * 1000, http_response.text))

        # json_response = json.loads(http_response.text)
        json_response = http_response.json()

        if ('response' not in json_response
                or 'status' not in json_response['response']
                or json_response['response']['status'] != 1):

            # raise self.cast_error(json_response)
            try:
                raise self.cast_error(json_response)
            except APIUsageExceededRateLimit:
                if self.retry_count > 1 and request.attempts < self.retry_count:
                    self.log('Retrying request: attempts %d!' % request.attempts)
                    time.sleep(0.25)
                    return self.send_request(request)
                else:
                    raise self.cast_error(json_response)

        return Response(request, json_response)

    def cast_error(self, response_body):
        if not 'response' in response_body or not 'status' in response_body['response']:
            return Error('Unexpected error: %r' % response_body)
        if 'API usage exceeded rate limit' in response_body['response']['errorMessage']:
            return APIUsageExceededRateLimit(response_body['response']['errorMessage'])
        return Error(response_body['response']['errorMessage'])

    def log(self, *args, **kwargs):
        '''Proxy access to the hasoffers logger, changing the level based on the debug setting'''
        logger.log(self.level, *args, **kwargs)

    def __getattr__(self, target):
        self.method_proxy.target = target
        return self.method_proxy


class Request(object):

    def __init__(self, target, method, params):
        self.target = target
        self.method = method
        self.params = params

        self.attempts = 0

        self.url = self.build_url(target, params)

    def build_url(self, target, params):
        base_url = ROOT + '%s.json?' % target
        return base_url + http_build_query(params)


class Response(object):

    def __init__(self, request, json_response):
        self.request = request
        self.json_response = json_response

        self.data = json_response['response']['data']
        self.status = json_response['response']['status']
        self.httpStatus = json_response['response']['httpStatus']
        self.errors = json_response['response']['errors']
        self.errorMessage = json_response['response']['errorMessage']

    def extract_all(self, model_name=None):
        model_name = model_name or self.request.target

        if 'page' in self.data:
            data = self.data['data']
        else:
            data = self.data

        return Mapper.extract_all(data, model_name)

    def extract_one(self, model_name=None):
        model_name = model_name or self.request.target

        return Mapper.extract_one(self.data, model_name)


class Model(object):

    def __init__(self, d):

        if not type(d) == dict:
            raise Exception('Неверный тип аругмента.')

        for k, v in d.items():
            if k == 'id':
                self.__dict__[k] = int(v)
            else:
                self.__dict__[k] = v


class Mapper(object):

    @classmethod
    def extract_one(cls, data, model_name):

        if not len(data):
            return None

        relative_scopes = {model: scope for model, scope in data.items() if model != model_name}
        model_scope = data[model_name]
        model_scope.update(relative_scopes)
        model = Model(model_scope)

        return model

    @classmethod
    def extract_all(cls, data, model_name):

        if not len(data):
            return None

        collection = []

        for object_id, object_scope in data.items():
            relative_scopes = {model: scope for model, scope in object_scope.items() if model != model_name}
            model_scope = object_scope[model_name]
            model_scope.update(relative_scopes)

            model = Model(model_scope)

            collection.append(model)

        return collection