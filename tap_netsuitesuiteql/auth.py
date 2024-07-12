import requests
from requests_oauthlib import OAuth1


class NetsuiteSuiteQLAuthenticator:
    def __init__(self, realm, client_key, client_secret, resource_owner_key, resource_owner_secret):
        self.realm = realm
        self.client_key = client_key
        self.client_secret = client_secret
        self.resource_owner_key = resource_owner_key
        self.resource_owner_secret = resource_owner_secret
        self.signature_method = 'HMAC-SHA256'
        self.signature_type = "AUTH_HEADER"

        self._oauth1 = OAuth1(client_key=self.client_key, client_secret=self.client_secret,
                              resource_owner_key=self.resource_owner_key,
                              resource_owner_secret=self.resource_owner_secret,
                              signature_type=self.signature_type, signature_method=self.signature_method,
                              realm=self.realm)

    def create_for_stream(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        return self._oauth1(request)

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        return self._oauth1(request)
