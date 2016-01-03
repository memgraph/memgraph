# -*- coding: utf-8 -*-

import unittest
import requests


class VertexCrudTest(unittest.TestCase):

    def check_reponse_status_code(self, r, code):
        '''
        Checks status code of the response and returns
        response data as json.

        Returns:
            response data (json)
        '''
        self.assertEqual(r.status_code, code)
        response = r.json()
        return response

    def check_metadata_and_id(self, response, id=None):
        '''
        Checks reponse id and return it.
        '''
        self.assertIn("metadata", response)
        metadata = response["metadata"]
        self.assertIn("id", metadata)
        response_id = metadata["id"]
        if id is not None:
            self.assertEqual(id, response_id)
        return response_id

    def check_response_data(self, response, data):
        '''
        Takes data from response json and compare them to the data
        argument.
        '''
        self.assertIn("data", response)
        response_data = response["data"]
        self.assertDictEqual(response_data, data)
        return response_data

    def check_read(self, resource_id, valid_data):
        '''
        Check a whole get request.
        '''
        resource_url = self.endpoint + '/%s' % resource_id
        r = requests.get(resource_url)
        response = self.check_reponse_status_code(r, 200)
        self.check_metadata_and_id(response, resource_id)
        self.check_response_data(response, valid_data)

    def test_crud(self):
        '''
        CRUD test:
            create -> read -> update -> read -> delete -> read
        '''
        self.endpoint = 'http://localhost:7474/db/data/node'

        create_payload = {"foo1": "bar1", "foo2": "bar2"}
        edit_payload = {"foo2": "bar22", "foo3": "bar3"}
        edited_resource = {"foo1": "bar1", "foo2": "bar22", "foo3": "bar3"}

        # 1. create
        r = requests.post(self.endpoint, json=create_payload)
        response = self.check_reponse_status_code(r, 201)
        self.resource_id = self.check_metadata_and_id(response)
        self.resource_url = self.endpoint + "/%s" % self.resource_id
        self.check_response_data(response, create_payload)

        # 2. read
        self.check_read(self.resource_id, create_payload)

        # 3. update
        r = requests.put(self.resource_url, edit_payload)
        self.check_reponse_status_code(r, 200)

        # 4. read
        self.check_read(self.resource_id, edited_resource)

        # 5. delete
        r = requests.delete(self.resource_url)
        self.check_response_status_code(r, 200)

        # 6. read
        r = requests.get(self.resource_url)
        self.check_response_status_code(r, 404)
